from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql.types import StructType
from datetime import datetime


class SuccessfulETLProcess(ETLProcess):  # pragma: no cover
    """
    Simplified ETLProcess used for testing integration flows in pipelines
    """

    OUTPUT_TABLE = "fake_etl_process_success"

    @classmethod
    def get_name(cls):
        return "Fake Successful ETL Process"

    def load_data(self, **kwargs):
        return dict()

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        data = self.spark.createDataFrame([], StructType([]))
        end_exec_time = datetime.now()
        return data, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=0,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )


class FailedETLProcess(ETLProcess):  # pragma: no cover
    """
    Simplified ETLProcess used for testing integration flows in pipelines. This ETLProcess will fail when called
    """

    OUTPUT_TABLE = "fake_etl_process_failed"

    @classmethod
    def get_name(cls):
        return "Faky Failed ETL Process"

    def load_data(self, **kwargs):
        return dict()

    def process(self, **kwargs):
        raise ValueError("FailedETLProcess failed (as expected)")
