from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple


class CheckmarkCaseMarkingCuratedProcess(CurationProcess):
    """
    Curated layer process for Checkmark case_marking — produces the Power BI
    consumption table pbi_case_marking from the harmonised case_marking table.

    Truncate-load semantics: SELECT * from harmonised, overwrite curated.
    """

    SOURCE_TABLE = "odw_harmonised_db.case_marking"
    OUTPUT_TABLE = "odw_curated_db.pbi_case_marking"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "checkmark-case-marking-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading harmonised data from {self.SOURCE_TABLE}")
        source_data = self.spark.sql(f"SELECT * FROM {self.SOURCE_TABLE}")
        return {
            "source_data": source_data,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        # Curated is a straight pass-through from harmonised — no transformation.
        # The legacy notebook does CREATE OR REPLACE TABLE ... AS SELECT *, which
        # becomes an overwrite write of the unchanged DataFrame.

        insert_count = df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "pbi_case_marking",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "checkmarkdata/pbi_case_marking",
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
