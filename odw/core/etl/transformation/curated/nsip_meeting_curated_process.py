from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


class NsipMeetingCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Meeting data from the harmonised layer.

    # Example usage via py_etl_executor

    ```
    input_arguments = {
        "etl_process_name": "NSIP Meeting Curation Process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_nsip_meeting"
    OUTPUT_TABLE = "nsip_meeting"

    @classmethod
    def get_name(cls) -> str:
        return "NSIP Meeting Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        Filters are applied here for performance (IsActive, meetingId IS NOT NULL).
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(
            f"Loading harmonised NSIP Meeting data from {self.HARMONISED_TABLE}"
        )
        harmonised_meeting = self.spark.sql(f"""
            SELECT
                caseId,
                caseReference,
                meetingAgenda,
                planningInspectorateRole,
                meetingId,
                meetingDate,
                meetingType
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
                AND meetingId IS NOT NULL
        """)
        return {
            "harmonised_meeting": harmonised_meeting,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Deduplicates by meetingId keeping the latest meetingDate.
        No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_meeting: DataFrame = self.load_parameter(
            "harmonised_meeting", source_data
        )

        # Deduplicate: keep latest per meetingId by meetingDate DESC
        window_dedup = Window.partitionBy("meetingId").orderBy(
            F.col("meetingDate").desc()
        )
        df = (
            harmonised_meeting.withColumn("rn", F.row_number().over(window_dedup))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .select(
                "caseId",
                "caseReference",
                "meetingAgenda",
                "planningInspectorateRole",
                "meetingId",
                "meetingDate",
                "meetingType",
            )
        )

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Meeting row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            f"odw_curated_db.{self.OUTPUT_TABLE}": {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": self.OUTPUT_TABLE,
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=f"odw_curated_db.{self.OUTPUT_TABLE}",
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
