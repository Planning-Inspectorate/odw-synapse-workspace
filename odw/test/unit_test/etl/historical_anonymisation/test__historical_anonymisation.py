from odw.test.util.test_case import SparkTestCase
from odw.test.util.util import (
    format_adls_path_to_local_path,
    get_all_files_in_directory,
)
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.core.etl.historical_anonymisation.historical_anonymisation_process import (
    HistoricalAnonymisationProcess,
)
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.io.synapse_data_io import SynapseDataIO
from pyspark.sql import DataFrame
from typing import List, Dict, Any
import json
import csv
import os
import pytest
import mock


class TestHistoricalAnonymisationProcess(SparkTestCase):
    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        with (
            mock.patch.object(LoggingUtil, "__new__"),
            mock.patch.object(LoggingUtil, "log_info", return_value=None),
            mock.patch.object(LoggingUtil, "log_error", return_value=None),
            mock.patch.object(
                Util,
                "get_storage_account",
                return_value="test-storage.dfs.core.windows.net",
            ),
            mock.patch.object(
                SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path
            ),
            mock.patch.object(
                HistoricalAnonymisationProcess,
                "get_all_files_in_directory",
                get_all_files_in_directory,
            ),
        ):
            yield

    def write_csv(self, data: Any, path: List[str]):
        directories = path[:-1]
        print(f"directories: '{directories}'")
        print(f"path: '{path}'")
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w+", newline="") as file:
            if data:
                keys = keys = data[0].keys()
                writer = csv.DictWriter(file, keys)
                writer.writeheader()
            else:
                writer = csv.writer(file)
            writer.writerows(data)

    def write_json(self, data: Any, path: List[str]):
        directories = path[-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w+", newline="") as file:
            json.dump(data, file)

    def test__historical_anonymisation__load_data__csv_raw_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        entity_name = "t_ha_ld_crd"
        raw_data_base_path = ("odw-raw", entity_name)
        raw_data_a = [
            {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_a, raw_data_base_path + ("2025-01-01", "raw_data_a.csv")
        )
        raw_data_b = [
            {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_b, raw_data_base_path + ("2025-01-02", "raw_data_b.csv")
        )

        standardised_data_base_path = f"odw-standardised/{entity_name}"
        standardised_data_a: DataFrame = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Picard",
                    "email": "myemail@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_a.write.format("parquet").mode("overwrite").save(
            f"{warehouse_name}/{standardised_data_base_path}"
        )
        standardised_data_b: DataFrame = spark.createDataFrame(
            [
                {
                    "id": 2,
                    "name": "Janeway",
                    "email": "otherEmail@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_b.write.format("parquet").mode("append").save(
            f"{warehouse_name}/{standardised_data_base_path}"
        )

        # todo check that data can be loaded
        expected_raw_data = spark.createDataFrame(
            [
                {"id": "1", "name": "Picard", "email": "myemail@someorg.co.uk"},
                {"id": "2", "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
            ]
        )
        expected_standardised_data = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Picard",
                    "email": "myemail@someorg.co.uk",
                    "layer": "standardised",
                },
                {
                    "id": 2,
                    "name": "Janeway",
                    "email": "otherEmail@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        override_config = {
            entity_name: {
                "raw_blob_path": entity_name,
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "Horizon",
                "raw_blob_read_options": {"header": "true"},
            }
        }
        with mock.patch.object(
            HistoricalAnonymisationProcess, "_ENTITY_CONFIG", override_config
        ):
            processor = HistoricalAnonymisationProcess(spark=spark)
            actual_loaded_data = processor.load_data(entity_name=entity_name)
            assert (
                "standardised_data" in actual_loaded_data
                and "raw_data" in actual_loaded_data
            )
            actual_standardised_data = actual_loaded_data.get("standardised_data")
            actual_raw_data = actual_loaded_data.get("raw_data")
            assert_dataframes_equal(
                expected_standardised_data, actual_standardised_data
            )
            assert_dataframes_equal(expected_raw_data, actual_raw_data)
