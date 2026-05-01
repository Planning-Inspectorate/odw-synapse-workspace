from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


_READING_CASE_COLUMNS = [
    "case_reference",
    "inspector_email",
    "inspector_name",
    "reader_email",
    "reader_name",
    "case_level",
    "date_submitted_by_inspector",
    "event_date",
    "type_of_reading",
    "procedure",
    "case_target_date",
    "reading_status",
    "able_to_read_decision",
    "advertisement",
    "appeal_against_conditions",
    "bespoke",
    "case_team_officer_email",
    "case_team_officer_name",
    "costs_decision",
    "green_belt",
    "kiosk",
    "linked",
    "neighbourhood_plan",
    "nsi_email",
    "nsi_name",
    "prior_approval",
    "reason_for_delay",
    "redetermination",
    "secretary_of_state_report",
    "spa_sac_eps",
    "special_category_data",
    "specialisms",
    "telecommunications",
    "urgent",
    "amendments_received_notification_needed",
    "able_to_check_amendments",
    "amendments_cycles",
    "amendments_target_date",
    "amendments_timeliness_review_flag",
    "date_amendments_checked",
    "date_amendments_received",
    "date_cleared",
    "date_decision_sent_for_issue",
    "date_read",
    "date_read_started",
    "decision_read",
    "delayed_amendments_submission",
    "delayed_submission",
    "document_version",
    "document_version_needed",
    "final_decision_notification_needed",
    "notification_sent_flag",
    "on_hold_notification_needed",
    "on_hold_status",
    "reading_needed_flag",
    "ready_for_issue",
    "reason_for_amendments_delay",
    "recall_notification_needed",
    "replacement_decision_flag",
    "return_count",
    "revised_decision_notification_needed",
    "skim_read",
    "source",
    "timeliness_review_flag",
    "date_modified",
]


class CheckmarkReadingCaseHarmonisationProcess(HarmonisationProcess):
    """
    Harmonisation process for Checkmark reading_case data.

    Truncate-load semantics: reads all rows from the standardised readingcase
    table, adds IngestionDate (timestamp) and an MD5-based RowID over all 65
    business columns, and overwrites the harmonised reading_case table.
    """

    SOURCE_TABLE = "odw_standardised_db.readingcase"
    OUTPUT_TABLE = "odw_harmonised_db.reading_case"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "checkmark-reading-case-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading source data from {self.SOURCE_TABLE}")
        source_data = self.spark.sql(f"SELECT * FROM {self.SOURCE_TABLE}")
        return {
            "source_data": source_data,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        self.spark.sql("SET spark.sql.legacy.timeParserPolicy = LEGACY")

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        df = df.select(*_READING_CASE_COLUMNS)

        # Transactional table — uses CURRENT_TIMESTAMP() to match legacy notebook
        df = df.withColumn("IngestionDate", F.current_timestamp())

        hash_inputs = [
            F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
            for c in _READING_CASE_COLUMNS
        ]
        df = df.withColumn("RowID", F.md5(F.concat_ws("|", *hash_inputs)))

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
                "table_name": "reading_case",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "reading_case",
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
