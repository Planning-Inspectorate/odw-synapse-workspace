from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class ListedBuildingCuratedProcess(CurationProcess):
    """
    ETL process for curating Listed Building data from the harmonised layer.

    Example usage via py_etl_orchestrator
    ```
    input_arguments = {
        "entity_stage_name": "listed-building-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.listed_building"
    OUTPUT_TABLE = "odw_curated_db.listed_building"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "listed-building-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data only.
        No transformations or joins applied here.
        """
        LoggingUtil().log_info(
            f"Loading harmonised Listed Building data from {self.HARMONISED_TABLE}"
        )

        harmonised_df = self.spark.sql(f"""
            SELECT
                CAST(entity AS LONG) AS entity,
                reference,
                name,
                listedBuildingGrade
            FROM {self.HARMONISED_TABLE}
            WHERE isActive = 'Y'
        """)

        return {
            "harmonised_listed_building": harmonised_df
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations.
        No reads or writes should happen here.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter(
            "harmonised_listed_building", source_data
        )

        # Apply any light curation rules here (if needed in future)
        curated_df = (
            df
            .select(
                F.col("entity"),
                F.col("reference"),
                F.col("name"),
                F.col("listedBuildingGrade")
            )
            .distinct()
        )

        insert_count = curated_df.count()

        LoggingUtil().log_info(
            f"Curated Listed Building row count: {insert_count}"
        )

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": curated_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "listed_building",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "listed_building",
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
                duration_seconds=(
                    end_exec_time - start_exec_time
                ).total_seconds(),
            )
        )