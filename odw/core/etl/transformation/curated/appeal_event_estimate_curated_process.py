from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime
from typing import Dict, Tuple


class AppealEventEstimateCuratedProcess(CurationProcess):
    """
    ETL process for curating Appeal Event Estimate data.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "etl_process_name": "appeal_event_estimate_curated_process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_event_estimate"
    OUTPUT_TABLE = "appeal_event_estimate"

    CURATED_COLUMNS = [
        "id",
        "caseReference",
        "preparationTime",
        "sittingTime",
        "reportingTime",
    ]

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_event_estimate_curated_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load harmonised Appeal Event Estimate data. All business rules are applied in `process`.
        """
        LoggingUtil().log_info(f"Loading harmonised Appeal Event Estimate data from {self.HARMONISED_TABLE}")
        harmonised_appeal_event_estimate = self.spark.sql(f"SELECT * FROM {self.HARMONISED_TABLE}")
        return {
            "harmonised_appeal_event_estimate": harmonised_appeal_event_estimate,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.

        Business logic (matches legacy notebook):
        - Filter rows where IsActive = 'Y'
        - Project the curated column set (any column missing on the source is emitted as null)
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised: DataFrame = self.load_parameter("harmonised_appeal_event_estimate", source_data)

        df = harmonised.where(F.col("IsActive") == F.lit("Y"))
        missing_columns = {col_name: F.lit(None).cast(StringType()) for col_name in self.CURATED_COLUMNS if col_name not in df.columns}
        if missing_columns:
            df = df.withColumns(missing_columns)
        df = df.select(*[F.col(col_name) for col_name in self.CURATED_COLUMNS])

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated Appeal Event Estimate row count: {insert_count}")

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
