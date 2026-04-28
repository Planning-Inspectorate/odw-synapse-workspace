from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


# Columns projected from the standardised table and used to build the MD5 RowID.
# Single source of truth for both the projection and the hash.
_CASE_MARKING_COLUMNS = [
    "ID",
    "case_reference",
    "overall_mark",
    "amendments_timeliness_mark",
    "complexity",
    "conditions_mark",
    "conditions_detail",
    "coverage_mark",
    "coverage_detail",
    "ground_a_mark",
    "ground_a_iit_level",
    "invalid_nullity",
    "invalid_nullity_mark",
    "legal_grounds_mark",
    "legal_grounds_considered",
    "non_legal_grounds_mark",
    "non_legal_grounds_considered",
    "other_grounds_affecting_complexity",
    "outcome",
    "overall_case_level_for_iit_progression",
    "presentation_accuracy_mark",
    "presentation_accuracy_detail",
    "structure_reasoning_mark",
    "structure_reasoning_detail",
    "timeliness_mark",
    "reading_complete_notification_needed",
]


class CheckmarkCaseMarkingHarmonisationProcess(HarmonisationProcess):
    """
    Harmonisation process for Checkmark case_marking data.

    Truncate-load semantics: reads the entire standardised table, adds
    IngestionDate and an MD5-based RowID, and overwrites the harmonised
    table.

    # Example usage via py_etl_orchestrator
    ```
    input_arguments = {
        "entity_stage_name": "checkmark-case-marking-harmonised",
        "debug": False
    }
    ```
    """

    SOURCE_TABLE = "odw_standardised_db.casemarking"
    OUTPUT_TABLE = "odw_harmonised_db.case_marking"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "checkmark-case-marking-harmonised"

    # ------------------------------------------------------------------
    # load_data — all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading source data from {self.SOURCE_TABLE}")
        source_data = self.spark.sql(f"SELECT * FROM {self.SOURCE_TABLE}")
        return {
            "source_data": source_data,
        }

    # ------------------------------------------------------------------
    # process — pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        # Carried over from legacy notebook for parsing legacy timestamps in source data
        self.spark.sql("SET spark.sql.legacy.timeParserPolicy = LEGACY")

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        # Project to exactly the columns we want, in the canonical order
        df = df.select(*_CASE_MARKING_COLUMNS)

        # Add IngestionDate (replaces CURRENT_TIMESTAMP() in legacy INSERT)
        df = df.withColumn("IngestionDate", F.current_timestamp())

        # Compute RowID as MD5 of all hashed columns
        # Mirror legacy: COALESCE(TRIM(CAST(col AS STRING)), '') joined by '|'
        hash_inputs = [
            F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
            for c in _CASE_MARKING_COLUMNS
        ]
        df = df.withColumn("RowID", F.md5(F.concat_ws("|", *hash_inputs)))

        # Data quality check matching the legacy notebook
        null_rowid_count = df.filter(F.col("RowID").isNull()).count()
        if null_rowid_count > 0:
            LoggingUtil().log_error(
                f"Data quality issue: {null_rowid_count} rows have NULL RowID values"
            )
        else:
            LoggingUtil().log_info("Data quality check passed: No NULL RowID values found")

        insert_count = df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "case_marking",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "case_marking",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }

        end_exec_time = datetime.now()
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
            ),
        )