from odw.core.io.data_io_factory import DataIOFactory
from odw.core.util.logging_util import LoggingUtil
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from odw.core.etl.etl_result import ETLResult, ETLFailResult
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.util.util import Util
from typing import List, Dict, Any, Tuple
from notebookutils import mssparkutils
import traceback
from datetime import datetime


class ETLProcess(ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        Return a unique name for the ETL process, to be identified as part of the factory

        :return str: A unique name for the process
        """

    @classmethod
    def load_parameter(cls, param_name: str, kwargs: Dict[str, Any], default: Any = None):
        param_value = kwargs.get(param_name, default)
        if param_value is None:
            raise ValueError(f"{cls.__name__} requires a {param_name} parameter to be provided, but was missing")
        return param_value

    @classmethod
    @LoggingUtil.logging_to_appins
    def get_all_files_in_directory(cls, source_path: str):
        files_to_explore = set(mssparkutils.fs.ls(source_path))
        found_files = set()
        while files_to_explore:
            next_item = files_to_explore.pop()
            if next_item.isDir:
                files_to_explore |= set(mssparkutils.fs.ls(next_item.path))
            else:
                found_files.add(next_item.path)
        return found_files
    
    @classmethod
    def load_orchestration_data(self):
        """
        Load the orchestration file
        """
        return SynapseFileDataIO().read(
            spark=SparkSession.builder.getOrCreate(),
            storage_name=Util.get_storage_account(),
            container_name="odw-config",
            blob_path=f"orchestration/orchestration.json",
            file_format="json",
            read_options={"multiline": "true"},
        )

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data using the given kwargs

        :param List[Dict[str, Any]] data_to_read: Data to read. List of entries that can each be fed into a relevant DataIO class
        :return Dict[str, DataFrame]: A dictionary of table names mapped to the underlying data
        """
        data_to_read: List[Dict[str, Any]] = kwargs.get("data_to_read", None)
        if not data_to_read:
            raise ValueError(f"ETLProcess expected a data_to_read parameter to be passed, but this was missing")
        data_map = dict()
        for metadata in data_to_read:
            data_name = metadata.get("data_name", None)
            storage_kind = metadata.get("storage_kind", None)
            data_format = metadata.get("data_format", None)
            if not data_name:
                raise ValueError(f"ETLProcess data_to_read expected a data_name parameter to be passed, but this was missing")
            if not storage_kind:
                raise ValueError(f"ETLProcess data_to_read expected a storage_kind parameter to be passed, but this was missing")
            if not data_format:
                raise ValueError(f"ETLProcess data_to_read expected a data_format parameter to be passed, but this was missing")
            data_io_inst = DataIOFactory.get(storage_kind)()
            data = data_io_inst.read(**metadata)
            data_map[data_name] = data
        return data_map

    @abstractmethod
    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Perform transformations on the given input data
        """
        pass

    def write_data(self, data_to_write: Dict[str, Any]):
        """
        Write the given dictionary of tables

        :param Dict[str, Any] data_to_write: A dictionary of the form <table_name, table_metadata>
                                             which can be fed into a relevant DataIO class
        """
        for table_name, table_metadata in data_to_write.items():
            storage_kind = table_metadata.get("storage_kind", None)
            file_format = table_metadata.get("file_format", None)
            if not storage_kind:
                raise ValueError(f"ETLProcess expected a storage_kind parameter to be passed, but this was missing")
            if not file_format:
                raise ValueError(f"ETLProcess expected a file_format parameter to be passed, but this was missing")
            data_io_inst = DataIOFactory.get(storage_kind)()
            data_io_inst.write(**table_metadata)

    def run(self, **kwargs) -> ETLResult:
        """
        Run the full ETL process with the given arguments

        Steps
        1. Load source data
        2. Perform transformations on the loaded data
        3. Write the transformed data

        If there is an exception detected at any point in the process, an ETLFailResult is
        returned containing the error trace. Otherwise an ETLSuccessResult is returned

        :param kwargs: Any arguments as specified in the concrete implementation classes
        :returns: An ETLResult. Exceptions are caught internally and are returned in the `metadata.exception` property of an ETLFailResult

        """
        etl_start_time = datetime.now()

        def generate_failure_result(start_time: datetime, exception: str, exception_trace=None, table_name=None):
            end_time = datetime.now()
            return ETLFailResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=start_time,
                    end_execution_time=end_time,
                    exception=exception,
                    exception_trace=exception_trace,
                    table_name=table_name,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    insert_count=0,
                    update_count=0,
                    delete_count=0,
                )
            )

        try:
            source_data_map = self.load_data(**kwargs)
            data_to_write, etl_result = self.process(source_data=source_data_map, **kwargs)
        except Exception as e:
            return generate_failure_result(etl_start_time, str(e), traceback.format_exc())
        if isinstance(etl_result, ETLFailResult):
            return etl_result
        try:
            self.write_data(data_to_write)
            return etl_result
        except Exception as e:
            return generate_failure_result(etl_start_time, str(e), traceback.format_exc(), table_name=", ".join(data_to_write.keys()))
