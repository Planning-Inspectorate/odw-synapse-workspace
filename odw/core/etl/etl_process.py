from odw.core.io.data_io_factory import DataIOFactory
from odw.core.util.logging_util import LoggingUtil
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from odw.core.etl.etl_result import ETLResult
from typing import List, Dict, Any
from notebookutils import mssparkutils


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

    def load_data(self, data_to_read: List[Dict[str, Any]]) -> Dict[str, DataFrame]:
        data_map = dict()
        for metadata in data_to_read:
            data_name = metadata.get("data_name", None)
            storage_kind = metadata.get("storage_kind", None)
            data_format = metadata.get("data_format", None)
            if not data_name:
                raise ValueError(f"ETLProcess expected a data_name parameter to be passed, but this was missing")
            if not storage_kind:
                raise ValueError(f"ETLProcess expected a storage_kind parameter to be passed, but this was missing")
            if not data_format:
                raise ValueError(f"ETLProcess expected a data_format parameter to be passed, but this was missing")
            data_io_inst = DataIOFactory.get(storage_kind)()
            data = data_io_inst.read(**metadata)
            data_map[data_name] = data
        return data_map

    @abstractmethod
    def process(self, **kwargs) -> ETLResult:
        pass

    def run(self, data_to_read: List[Dict[str, Any]]):
        source_data_map = self.load_data(data_to_read)
        return self.process(source_data=source_data_map)
