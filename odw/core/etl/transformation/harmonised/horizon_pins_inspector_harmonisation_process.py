from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from datetime import datetime
from typing import Dict, Tuple


class HorizonPinsInspectorHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for building the SCD-2 harmonised Horizon Inspector table.

    Reads from odw_standardised_db.horizon_pins_inspector, deduplicates columns,
    derives IngestionDate, and builds a full SCD-2 timeline keyed by horizonId.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "horizon-pins-inspector-harmonised",
        "debug": False
    }
    ```
    """

    HORIZON_TABLE = "odw_standardised_db.horizon_pins_inspector"
    STAGE_TABLE = "odw_harmonised_db.pins_inspector_stg"
    OUTPUT_TABLE = "odw_harmonised_db.horizon_pins_inspector"

    # Columns excluded from the SCD-2 state hash (plumbing / tracking fields)
    _HASH_EXCLUDE = frozenset({"RowID", "ValidTo", "IsActive", "IngestionDate", "ODTSourceSystem", "Migrated", "SourceSystemID"})

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "horizon-pins-inspector-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        return {"horizon_data": self.spark.table(self.HORIZON_TABLE)}

    def _clean_source(self, hzn_src: DataFrame) -> DataFrame:
        # Remove case-insensitive duplicate column names, keeping the first occurrence
        seen: dict = {}
        exprs = []
        for col_name in hzn_src.columns:
            lc = col_name.lower()
            if lc in seen:
                continue
            seen[lc] = col_name
            exprs.append(F.col(col_name).alias(col_name))
        hzn_src = hzn_src.select(*exprs)

        # Derive IngestionDate if absent: prefer expected_from, else current_timestamp
        if "IngestionDate" not in hzn_src.columns:
            ing_col = F.col("expected_from") if "expected_from" in hzn_src.columns else F.current_timestamp()
            hzn_src = hzn_src.withColumn("IngestionDate", ing_col)

        return (
            hzn_src
            .withColumn("Migrated", F.lit("0"))
            .withColumn("ODTSourceSystem", F.lit("HORIZON"))
            .withColumn("ValidTo", F.lit(None).cast("string"))
            .withColumn("RowID", F.lit(""))
            .withColumn("IsActive", F.lit("N"))
            .withColumn("SourceSystemID", F.lit("Inspectors"))
            .filter(F.col("horizonId").isNotNull())
        )

    def _build_scd2(self, stg: DataFrame) -> DataFrame:
        stg = stg.withColumn("IngestionDate", F.col("IngestionDate").cast("timestamp"))

        hash_cols = [c for c in stg.columns if c not in self._HASH_EXCLUDE]
        state_hash = F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]),
            256,
        )

        norm_row_id = F.when(
            F.col("RowID").isNull() | (F.col("RowID") == ""), F.lit("~")
        ).otherwise(F.col("RowID"))

        # tie-breaker row_number within (horizonId, IngestionDate, ODTSourceSystem)
        w_tie = W.partitionBy("horizonId", "IngestionDate", "ODTSourceSystem").orderBy(
            F.col("normRowID").asc(),
            F.col("fallback_hash").asc(),
        )

        ordered = (
            stg.withColumn("state_hash", state_hash)
            .withColumn("normRowID", norm_row_id)
            .withColumn("fallback_hash", state_hash)
            .withColumn("tie", F.row_number().over(w_tie))
        )

        # Collapse exact duplicates within (horizonId, IngestionDate, ODTSourceSystem, state_hash)
        w_dup = W.partitionBy("horizonId", "IngestionDate", "ODTSourceSystem", "state_hash").orderBy(
            F.col("normRowID").asc(),
            F.col("fallback_hash").asc(),
        )
        ordered = (
            ordered.withColumn("dup_rn", F.row_number().over(w_dup))
            .filter(F.col("dup_rn") == 1)
            .drop("dup_rn")
        )

        # Timeline ordering within each inspector
        w_ing = W.partitionBy("horizonId").orderBy(
            F.col("IngestionDate").asc(),
            F.col("normRowID").asc(),
            F.col("fallback_hash").asc(),
            F.col("tie").asc(),
        )

        # Mark state changes (new row or hash differs from previous)
        scd_base = ordered.withColumn("prev_hash", F.lag("state_hash").over(w_ing)).withColumn(
            "chg",
            F.when(F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("state_hash")), 1).otherwise(0),
        )

        changes = scd_base.filter(F.col("chg") == 1)

        # Derive ValidTo from the next change point; bump by 1 µs when timestamps collide
        w_change = W.partitionBy("horizonId").orderBy(
            F.col("IngestionDate").asc(),
            F.col("normRowID").asc(),
            F.col("fallback_hash").asc(),
            F.col("tie").asc(),
        )

        scd2 = (
            changes.withColumn("ValidFrom", F.col("IngestionDate"))
            .withColumn("next_IngestionDate", F.lead("IngestionDate").over(w_change))
            .withColumn(
                "ValidTo",
                F.when(F.col("next_IngestionDate").isNull(), F.lit(None).cast("timestamp"))
                .when(
                    F.col("next_IngestionDate") == F.col("IngestionDate"),
                    F.col("next_IngestionDate") + F.expr("INTERVAL 1 MICROSECOND"),
                )
                .otherwise(F.col("next_IngestionDate")),
            )
            .withColumn(
                "IsActive",
                F.when(F.col("next_IngestionDate").isNull(), F.lit("Y")).otherwise(F.lit("N")),
            )
            .drop("prev_hash", "chg", "next_IngestionDate", "ValidFrom")
        )

        return scd2.drop("normRowID", "fallback_hash", "tie", "state_hash")

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()
        source_data = self.load_parameter("source_data", kwargs)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)

        stg_df = self._clean_source(horizon_data)
        scd2 = self._build_scd2(stg_df)

        insert_count = scd2.count()
        LoggingUtil().log_info(f"Horizon PINS Inspector SCD-2 row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.STAGE_TABLE: {
                "data": stg_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "pins_inspector_stg",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "pins_inspector_stg",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            },
            self.OUTPUT_TABLE: {
                "data": scd2,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "horizon_pins_inspector",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "horizon_pins_inspector",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["IsActive"],
            },
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
