from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime
from typing import Dict, Tuple
import json


# Columns added by HorizonStandardisationProcess and the harmonisation plumbing.
# These are excluded from the state hash so that only true business-data changes
# trigger a new SCD-2 row.
_EXCLUDE_FROM_HASH = frozenset(
    [
        # harmonisation plumbing
        "RowID",
        "ValidTo",
        "IsActive",
        "IngestionDate",
        "ODTSourceSystem",
        "Migrated",
        "SourceSystemID",
        # standardisation metadata written by HorizonStandardisationProcess
        "ingested_datetime",
        "ingested_by_process_name",
        "expected_from",
        "expected_to",
        "input_file",
        "modified_datetime",
        "modified_by_process_name",
        "entity_name",
        "file_ID",
    ]
)


class HorizonHarmonisationProcess(HarmonisationProcess):
    """
    Generic ETL process for harmonising Horizon file-based data into an SCD-2 harmonised table.

    Reads configuration from the orchestration file to determine source/target tables and the
    business primary key, then applies state-hash-based SCD-2 logic (keeping only rows where
    the business state has changed) before writing to the harmonised layer.

    Differences from ServiceBusHarmonisationProcess
    ------------------------------------------------
    - No ``message_type`` field: state changes are detected by hashing business columns.
    - Duplicate column names (case-insensitive) are collapsed — a Horizon CSV artefact.
    - ``IngestionDate`` is derived from ``expected_from`` (file batch date), not message timestamps.
    - ``ODTSourceSystem`` is always ``'HORIZON'``, ``Migrated`` is always ``'0'``.

    Required orchestration fields
    -----------------------------
    - ``Source_Filename_Start``: used as the ``entity_name`` lookup key.
    - ``Standardised_Table_Name``: source table in ``odw_standardised_db``.
    - ``Harmonised_Table_Name``: target table in ``odw_harmonised_db``.
    - ``Entity_Primary_Key``: business key used for SCD-2 partitioning.

    Optional orchestration fields
    -----------------------------
    - ``Source_System_ID``: value written to the ``SourceSystemID`` column (omitted if absent).

    # Example usage via py_etl_orchestrator

    ```python
    input_arguments = {
        "entity_stage_name": "Horizon Harmonisation",
        "entity_name": "horizon_pins_inspector",
    }
    ```
    """

    STD_DB = "odw_standardised_db"
    HRM_DB = "odw_harmonised_db"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "Horizon Harmonisation"

    # ------------------------------------------------------------------
    # load_data — all reads happen here, no joins or transformations
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        entity_name: str = self.load_parameter("entity_name", kwargs)

        orchestration_data = self.load_orchestration_data()
        definitions: list = json.loads(orchestration_data.toJSON().first())["definitions"]
        definition: dict = next((d for d in definitions if entity_name == d.get("Source_Filename_Start")), None)
        if not definition:
            raise RuntimeError(f"No orchestration definition found for 'Source_Filename_Start' == '{entity_name}'")

        std_table = definition["Standardised_Table_Name"]
        full_std_table = f"{self.STD_DB}.{std_table}"

        LoggingUtil().log_info(f"Loading Horizon standardised data from {full_std_table}")
        source_data = self.spark.table(full_std_table)

        return {
            "orchestration_data": orchestration_data,
            "source_data": source_data,
        }

    # ------------------------------------------------------------------
    # process — pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        entity_name: str = self.load_parameter("entity_name", kwargs)
        source_data_map: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)

        orchestration_data: DataFrame = self.load_parameter("orchestration_data", source_data_map)
        raw: DataFrame = self.load_parameter("source_data", source_data_map)

        definitions: list = json.loads(orchestration_data.toJSON().first())["definitions"]
        definition: dict = next((d for d in definitions if entity_name == d.get("Source_Filename_Start")), None)
        if not definition:
            raise RuntimeError(f"No orchestration definition found for 'Source_Filename_Start' == '{entity_name}'")

        hrm_table: str = definition["Harmonised_Table_Name"]
        primary_key: str = definition["Entity_Primary_Key"]
        source_system_id: str = definition.get("Source_System_ID", None)
        full_hrm_table = f"{self.HRM_DB}.{hrm_table}"

        # Step 1: Collapse case-insensitive duplicate column names (Horizon CSV artefact).
        seen: dict = {}
        exprs = []
        for col_name in raw.columns:
            lc = col_name.lower()
            if lc in seen:
                continue
            seen[lc] = col_name
            exprs.append(F.col(col_name).alias(col_name))
        data = raw.select(*exprs)

        # Step 2: Derive IngestionDate from expected_from (preferred) or ingested_datetime.
        if "IngestionDate" not in data.columns:
            if "expected_from" in data.columns:
                ing_col = F.col("expected_from").cast("timestamp")
            elif "ingested_datetime" in data.columns:
                ing_col = F.col("ingested_datetime").cast("timestamp")
            else:
                ing_col = F.current_timestamp()
            data = data.withColumn("IngestionDate", ing_col)

        # Step 3: Add harmonisation plumbing columns.
        data = (
            data.withColumn("Migrated", F.lit("0"))
            .withColumn("ODTSourceSystem", F.lit("HORIZON"))
            .withColumn("ValidTo", F.lit(None).cast("string"))
            .withColumn("RowID", F.lit(""))
            .withColumn("IsActive", F.lit("N"))
        )
        if source_system_id is not None:
            data = data.withColumn("SourceSystemID", F.lit(source_system_id))

        # Step 4: Drop rows where the business key is null.
        data = data.filter(F.col(primary_key).isNotNull())

        # Step 5: Compute a state hash over all business columns (excludes metadata).
        hash_cols = [c for c in data.columns if c not in _EXCLUDE_FROM_HASH]
        state_hash_expr = F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]),
            256,
        )

        # Tie-breaking helpers for deterministic ordering within the same key + timestamp.
        norm_row_id = F.when(F.col("RowID").isNull() | (F.col("RowID") == ""), F.lit("~")).otherwise(F.col("RowID"))
        fallback_hash = state_hash_expr

        w_tie = Window.partitionBy(primary_key, "IngestionDate", "ODTSourceSystem").orderBy(norm_row_id.asc(), fallback_hash.asc())
        data = (
            data.withColumn("_state_hash", state_hash_expr)
            .withColumn("_norm_row_id", norm_row_id)
            .withColumn("_fallback_hash", fallback_hash)
            .withColumn("_tie", F.row_number().over(w_tie))
        )

        # Step 6: Collapse exact duplicates within (key, IngestionDate, ODTSourceSystem, state_hash).
        w_dup = Window.partitionBy(primary_key, "IngestionDate", "ODTSourceSystem", "_state_hash").orderBy(
            F.col("_norm_row_id").asc(), F.col("_fallback_hash").asc()
        )
        data = data.withColumn("_dup_rn", F.row_number().over(w_dup)).filter(F.col("_dup_rn") == 1).drop("_dup_rn")

        # Step 7: Keep only rows where the business state has changed (SCD-2 change detection).
        w_ing = Window.partitionBy(primary_key).orderBy(
            F.col("IngestionDate").asc(),
            F.col("_norm_row_id").asc(),
            F.col("_fallback_hash").asc(),
            F.col("_tie").asc(),
        )
        data = (
            data.withColumn("_prev_hash", F.lag("_state_hash").over(w_ing))
            .withColumn(
                "_chg",
                F.when(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_state_hash")), 1).otherwise(0),
            )
            .filter(F.col("_chg") == 1)
        )

        # Step 8: Compute ValidTo as the IngestionDate of the next change point.
        #         If two changes share the same timestamp, nudge by 1 microsecond to preserve ordering.
        w_change = Window.partitionBy(primary_key).orderBy(
            F.col("IngestionDate").asc(),
            F.col("_norm_row_id").asc(),
            F.col("_fallback_hash").asc(),
            F.col("_tie").asc(),
        )
        next_ing = F.lead("IngestionDate").over(w_change)
        data = data.withColumn(
            "ValidTo",
            F.when(next_ing.isNull(), F.lit(None).cast("timestamp"))
            .when(next_ing == F.col("IngestionDate"), next_ing + F.expr("INTERVAL 1 MICROSECOND"))
            .otherwise(next_ing),
        ).withColumn(
            "IsActive",
            F.when(next_ing.isNull(), F.lit("Y")).otherwise(F.lit("N")),
        )

        # Step 9: Drop all helper columns.
        data = data.drop("_state_hash", "_norm_row_id", "_fallback_hash", "_tie", "_prev_hash", "_chg")

        insert_count = data.count()

        data_to_write = {
            full_hrm_table: {
                "data": data,
                "storage_kind": "ADLSG2-Table",
                "database_name": self.HRM_DB,
                "table_name": hrm_table,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": hrm_table,
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["IsActive"],
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=full_hrm_table,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
