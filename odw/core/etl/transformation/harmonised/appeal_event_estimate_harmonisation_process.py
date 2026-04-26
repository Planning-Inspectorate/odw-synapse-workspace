from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple


class AppealEventEstimateHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising Appeal Event Estimate data.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal_event_estimate_harmonisation_process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_event_estimate"
    OUTPUT_TABLE = "odw_curated_db.appeal_event_estimate"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_event_estimate_harmonisation_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed from harmonised.
        """
        LoggingUtil().log_info(f"Loading harmonised Appeal Event Estimate data from {self.HARMONISED_TABLE}")
        harmonised_appeal_event_estimate = self.spark.sql(f"""
            SELECT
                id,
                caseReference,
                preparationTime,
                sittingTime,
                reportingTime
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        return {
            "harmonised_appeal_event_estimate": harmonised_appeal_event_estimate,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Reads from harmonised Appeal Event Estimate and writes to curated.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        dataFrame: DataFrame = self.load_parameter("harmonised_appeal_event_estimate", source_data)

        insert_count = dataFrame.count()
        LoggingUtil().log_info(f"Curated Appeal Event Estimate row count: {insert_count}")

        df = self.spark.sql(f"DESCRIBE EXTENDED {self.OUTPUT_TABLE}")
        rows = df.filter(df.col_name == "Location").select("data_type").collect()
        table_path = rows[0]["data_type"] if rows else None

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": dataFrame,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_event_estimate",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "appeal_event_estimate",
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
