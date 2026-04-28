from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Any


class CheckmarkCaseMarkingCuratedProcess(CurationProcess):
    """
    Curated layer process for Checkmark case_marking — produces the Power BI
    consumption table pbi_case_marking from the harmonised case_marking table.
    """

    OUTPUT_TABLE = "pbi_case_marking"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.hrm_db: str = "odw_harmonised_db"
        self.cur_db: str = "odw_curated_db"

    @classmethod
    def get_name(cls):
        return "Checkmark Case Marking Curated"

    def load_data(self, **kwargs):
        source_table_path = f"{self.hrm_db}.case_marking"
        LoggingUtil().log_info(f"Loading harmonised data from {source_table_path}")

        source_data: DataFrame = self.spark.sql(f"SELECT * FROM {source_table_path}")
        return {
            "source_data": source_data,
        }

    def process(self, **kwargs):
        start_exec_time = datetime.now()

        source_data: dict = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        # Curated layer is currently a straight pass-through from harmonised.
        #  CREATE OR REPLACE TABLE ... AS SELECT *
        

        insert_count = df.count()
        curated_table_path = f"{self.cur_db}.{self.OUTPUT_TABLE}"

        data_to_write = {
            curated_table_path: {
                "data": df,
                "storage_kind": "ADLSG2-Delta",
                "database_name": self.cur_db,
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "checkmarkdata/pbi_case_marking",  # mirrors legacy ABFSS path
                "write_mode": "overwrite",
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=curated_table_path,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            ),
        )