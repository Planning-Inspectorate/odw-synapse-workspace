from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from datetime import datetime
from typing import Dict, Tuple


class AppealEventCuratedMipinsProcess(CurationProcess):
    HARMONISED_TABLE = "odw_harmonised_db.appeal_event"
    OUTPUT_TABLE = "odw_curated_db.appeal_event_curated_mipins"

    TIMESTAMP_COLUMNS = [
        "eventStartDateTime",
        "eventEndDateTime",
        "NotificationOfSiteVisit",
        "IngestionDate",
        "ValidTo",
    ]

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_event_curated_mipins"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading harmonised Appeal Event data from {self.HARMONISED_TABLE}")

        df = self.spark.sql(f"""
            SELECT
                eventId,
                caseReference,
                eventType,
                eventName,
                eventStatus,
                CAST(IsUrgent AS BOOLEAN) AS IsUrgent,
                CAST(eventPublished AS BOOLEAN) AS eventPublished,
                eventStartDateTime,
                eventEndDateTime,
                NotificationOfSiteVisit,
                addressLine1,
                addressLine2,
                addressTown,
                addressCounty,
                addressPostcode,
                Migrated,
                ODTSourceSystem,
                SourceSystemID,
                IngestionDate,
                ValidTo,
                RowID,
                IsActive
            FROM {self.HARMONISED_TABLE}
            WHERE
                ODTSourceSystem = 'ODT'
                AND (eventStartDateTime IS NULL OR eventStartDateTime >= TIMESTAMP('1900-01-01'))
                AND (eventEndDateTime IS NULL OR eventEndDateTime >= TIMESTAMP('1900-01-01'))
                AND (NotificationOfSiteVisit IS NULL OR NotificationOfSiteVisit >= TIMESTAMP('1900-01-01'))
                AND (IngestionDate IS NULL OR IngestionDate >= TIMESTAMP('1900-01-01'))
                AND (ValidTo IS NULL OR ValidTo >= TIMESTAMP('1900-01-01'))
        """)

        return {"harmonised_appeal_event": df}

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("harmonised_appeal_event", source_data)

        for col in self.TIMESTAMP_COLUMNS:
            if col in df.columns:
                df = df.withColumn(col, F.to_timestamp(F.from_utc_timestamp(F.col(col), "Europe/London"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated Appeal Event (MIPINS) row count: {insert_count}")

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_event_curated_mipins",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "appeal_event_curated_mipins",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }

        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.get_name(),
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
