from odw.test.util.mock.import_mock_notebook_utils import notebookutils  # noqa: F401
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path
from odw.test.util.util import format_adls_path_to_local_path, format_to_adls_path
from odw.test.util.test_case import TestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import SparkSession, DataFrame
import json
import csv
from typing import List, Dict, Any
import os
import mock
import pytest
import logging


class ETLTestCase(TestCase):
    def session_setup(self):
        return super().session_setup()

    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        with mock.patch("notebookutils.mssparkutils.runtime.context", {"pipelinejobid": "some_guid", "isForPipeline": True}):
            with mock.patch.object(SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path):
                with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                    with mock.patch.object(Util, "get_path_to_file", generate_local_path):
                        with mock.patch.object(LoggingUtil, "__new__"):
                            with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                                with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                                    yield

    def write_csv(self, csv_data: List[List[Any]], path: List[str]):
        directories = path[:-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w", newline="") as file:
            writer = csv.writer(file)
            # Write data to the CSV file
            writer.writerows(csv_data)

    def write_json(self, json_data: Dict[str, Any], path: List[str]):
        directories = path[:-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w", newline="") as file:
            json.dump(json_data, file, indent=4)

    def write_existing_table(
        self,
        spark: SparkSession,
        data: DataFrame,
        table_name: str,
        database_name: str,
        container: str,
        blob_path: str,
        mode: str,
        options: Dict[str, Any] = dict(),
    ):
        logging.info(f"Createing table '{database_name}.{table_name}'")
        spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        table_path = f"{database_name}.{table_name}"
        data_path = format_to_adls_path(None, container, blob_path)
        write_opts = options | {"path": data_path}
        writer = data.write.format("delta").mode(mode)
        for option, value in write_opts.items():
            writer = writer.option(option, value)
        writer.saveAsTable(table_path)
