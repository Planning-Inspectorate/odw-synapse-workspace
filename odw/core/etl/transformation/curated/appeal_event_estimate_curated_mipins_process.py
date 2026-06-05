from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class AppealEventEstimateCuratedMipinsProcess(CurationProcess):
    """
    ETL process for curating Appeal Event Estimate data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "etl_process_name": "Appeal Event Estimate MIPINS Curation Process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_event_estimate"
    OUTPUT_TABLE = "appeal_event_estimate_curated_mipins"
    CURATED_COLUMNS = [
        "AppealsEstimateEventID",
        "ID",
        "caseReference",
        "preparationTime",
        "sittingTime",
        "reportingTime",
        "migrated",
        "ODTSourceSystem",
        "SourceSystemID",
        "IngestionDate",
        "ValidTo",
        "ROWID",
        "ISActive",
    ]

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "Appeal Event Estimate MIPINS Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading harmonised Appeal Event Estimate data from {self.HARMONISED_TABLE}")
        harmonised_appeal_event_estimate = self.spark.sql(f"SELECT * FROM {self.HARMONISED_TABLE}")

        return {
            "harmonised_appeal_event_estimate": harmonised_appeal_event_estimate,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Converts UTC timestamp columns to Europe/London timezone.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("harmonised_appeal_event_estimate", source_data)

        cutoff = F.lit("1900-01-01").cast("timestamp")
        df = (
            df.where(
                (F.col("ODTSourceSystem") == F.lit("ODT"))
                & (F.col("IngestionDate").isNull() | (F.col("IngestionDate") >= cutoff))
                & (F.col("ValidTo").isNull() | (F.col("ValidTo") >= cutoff))
            )
            .select(*[F.col(c) for c in self.CURATED_COLUMNS])
            .distinct()
        )

        df = df.withColumns(
            {
                column_name: F.to_timestamp(
                    F.from_utc_timestamp(F.col(column_name), "Europe/London"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS",
                )
                for column_name in ["IngestionDate", "ValidTo"]
            }
        )

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated Appeal Event Estimate MIPINS row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
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
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
