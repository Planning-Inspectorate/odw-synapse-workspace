from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from datetime import datetime
from typing import Any


class CheckmarkCaseMarkingHarmonisationProcess(HarmonisationProcess):
    """
    Harmonisation process for Checkmark case_marking data.
    
    Reads from odw_standardised_db.casemarking, applies full refresh into
    odw_harmonised_db.case_marking with an MD5-based RowID and IngestionDate.
    """

    OUTPUT_TABLE = "case_marking"

    # The 26 columns that participate in both the projection and the RowID hash.
    # Keeping these in one list avoids drift between INSERT and the hash later.
    HASHED_COLUMNS = [
        "ID", "case_reference", "overall_mark", "amendments_timeliness_mark",
        "complexity", "conditions_mark", "conditions_detail", "coverage_mark",
        "coverage_detail", "ground_a_mark", "ground_a_iit_level",
        "invalid_nullity", "invalid_nullity_mark", "legal_grounds_mark",
        "legal_grounds_considered", "non_legal_grounds_mark",
        "non_legal_grounds_considered", "other_grounds_affecting_complexity",
        "outcome", "overall_case_level_for_iit_progression",
        "presentation_accuracy_mark", "presentation_accuracy_detail",
        "structure_reasoning_mark", "structure_reasoning_detail",
        "timeliness_mark", "reading_complete_notification_needed",
    ]

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"

    @classmethod
    def get_name(cls):
        return "Checkmark Case Marking Harmonisation"

    def load_data(self, **kwargs):
        # Read the entire standardised table — full refresh pattern.
        source_table_path = f"{self.std_db}.casemarking"
        LoggingUtil().log_info(f"Loading source data from {source_table_path}")

        source_data: DataFrame = self.spark.sql(f"SELECT * FROM {source_table_path}")

        return {
            "source_data": source_data,
        }

    def _build_rowid_hash(self, df: DataFrame) -> DataFrame:
        """
        Mirror of the legacy MD5(concat_ws('|', COALESCE(TRIM(CAST(...))))) pattern,
        applied across the 26 hashed columns.
        """
        hash_inputs = [
            F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
            for c in self.HASHED_COLUMNS
        ]
        return df.withColumn("RowID", F.md5(F.concat_ws("|", *hash_inputs)))

    def process(self, **kwargs):
        start_exec_time = datetime.now()

        # Spark session config carried over from legacy notebook
        self.spark.sql("SET spark.sql.legacy.timeParserPolicy = LEGACY")

        source_data: dict = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        # Project the columns we keep, in the order the harmonised table expects
        df = df.select(*self.HASHED_COLUMNS)

        # Add the harmonised-layer columns the legacy INSERT was producing
        df = df.withColumn("IngestionDate", F.current_timestamp())
        df = self._build_rowid_hash(df)

        # Data quality check — same as legacy null RowID check
        null_rowid_count = df.filter(F.col("RowID").isNull()).count()
        if null_rowid_count > 0:
            LoggingUtil().log_error(
                f"Data quality issue: {null_rowid_count} rows have NULL RowID values"
            )
        else:
            LoggingUtil().log_info("Data quality check passed: No NULL RowID values found")

        insert_count = df.count()
        harmonised_table_path = f"{self.hrm_db}.{self.OUTPUT_TABLE}"

        data_to_write = {
            harmonised_table_path: {
                "data": df,
                # FULL REFRESH semantics — overwrite the table on every run.
                # 
                #    the available DataIO subclasses (might be "ADLSG2-Delta-Overwrite"
                #    or there may be a "write_mode": "overwrite" key).
                "storage_kind": "ADLSG2-Delta",
                "database_name": self.hrm_db,
                "table_name": self.OUTPUT_TABLE,
                #  Storage endpoint, container, blob path — the legacy notebook
                #    didn't manage these for harmonised (the table was managed by Spark).
                #    Need to confirm whether harmonised tables in the new framework
                #    require an explicit ADLS path or whether the framework derives it.
                "write_mode": "overwrite",
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=harmonised_table_path,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,  # Note: legacy did DELETE FROM; reframed as overwrite
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            ),
        )