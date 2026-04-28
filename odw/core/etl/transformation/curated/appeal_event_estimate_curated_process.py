from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class AppealEventEstimateCuratedProcess(CurationProcess):
    """
    ETL process for curating Appeal Event Estimate data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal_event_estimate_curated_process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_event_estimate"
    OUTPUT_TABLE = "odw_curated_db.appeal_event_estimate_curated_mipins"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_event_estimate_curated_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed from service bus.
        """
        LoggingUtil().log_info(f"Loading harmonised Appeal Event Estimate data from {self.HARMONISED_TABLE}")
        harmonised_appeal_event_estimate = self.spark.sql(f"""
            SELECT DISTINCT
                AppealsEstimateEventID,
                ID,
                caseReference,
                preparationTime,
                sittingTime,
                reportingTime,
                migrated,
                ODTSourceSystem,
                SourceSystemID,
                IngestionDate,
                ValidTo,
                ROWID,
                ISActive
            FROM {self.HARMONISED_TABLE}
            WHERE
                ODTSourceSystem = 'ODT'
                AND (IngestionDate IS NULL OR IngestionDate >= TIMESTAMP('1900-01-01'))
                AND (ValidTo IS NULL OR ValidTo >= TIMESTAMP('1900-01-01'))
        """)

        # Resolve the output table path so process() stays pure transformation.
        table_path = self._resolve_table_path()

        return {
            "harmonised_appeal_event_estimate": harmonised_appeal_event_estimate,
            "table_path": table_path,
        }

    def _resolve_table_path(self):
        """Resolve the Delta table location via DESCRIBE EXTENDED."""
        df = self.spark.sql(f"DESCRIBE EXTENDED {self.OUTPUT_TABLE}")
        rows = df.filter(df.col_name == "Location").select("data_type").collect()
        return rows[0]["data_type"] if rows else None

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Converts UTC timestamp columns to Europe/London timezone.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        dataFrame: DataFrame = self.load_parameter("harmonised_appeal_event_estimate", source_data)
        table_path = self.load_parameter("table_path", source_data)

        # Convert UTC timestamps to Europe/London timezone
        timestamp_columns = ["IngestionDate", "ValidTO"]
        for column_name in timestamp_columns:
            dataFrame = dataFrame.withColumn(
                column_name,
                F.to_timestamp(
                    F.from_utc_timestamp(F.col(column_name), "Europe/London"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS",
                ),
            )

        insert_count = dataFrame.count()
        LoggingUtil().log_info(f"Curated Appeal Event Estimate MIPINS row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": dataFrame,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_event_estimate_curated_mipins",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "appeal_event_estimate_curated_mipins",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
                "path": table_path,
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
