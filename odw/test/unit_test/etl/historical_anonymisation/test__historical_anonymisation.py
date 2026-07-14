from odw.test.util.test_case import SparkTestCase
from odw.test.util.util import (
    format_adls_path_to_local_path,
    get_all_files_in_directory,
)
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.core.etl.historical_anonymisation.historical_anonymisation_process import (
    HistoricalAnonymisationProcess,
)
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.core.anonymisation.engine import AnonymisationEngine
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
        directories = path[:-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w+", newline="") as file:
            json.dump(data, file)

    def test__historical_anonymisation__load_data__horizon_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        subfolder = "t_ha_ld_hd"
        entity_name = "t_ha_ld_hd__main"
        other_entity_name = "t_ha_ld_hd__other"
        raw_data_a = [
            {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_a,
            ("odw-raw", subfolder, "2025-01-01", entity_name, "raw_data_a.csv"),
        )
        raw_data_b = [
            {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_b,
            ("odw-raw", subfolder, "2025-01-02", entity_name, "raw_data_b.csv"),
        )
        other_entity_data = [{"id": 1, "name": "Locutus", "email": "locutus@borg.com"}]
        self.write_csv(
            other_entity_data,
            ("odw-raw", subfolder, "2025-01-01", other_entity_name, "raw_data_a.csv"),
        )
        standardised_data_base_path = "odw-standardised/"
        standardised_data_main_a: DataFrame = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Picard",
                    "email": "myemail@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_main_a.write.format("parquet").mode("overwrite").save(
            f"{warehouse_name}/{standardised_data_base_path}{entity_name}"
        )
        standardised_data_main_b: DataFrame = spark.createDataFrame(
            [
                {
                    "id": 2,
                    "name": "Janeway",
                    "email": "otherEmail@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_main_b.write.format("parquet").mode("append").save(
            f"{warehouse_name}/{standardised_data_base_path}{entity_name}"
        )
        standardised_data_other_a: DataFrame = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Locutus",
                    "email": "locutus@borg.com",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_other_a.write.format("parquet").mode("overwrite").save(
            f"{warehouse_name}/{standardised_data_base_path}/{other_entity_name}"
        )
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
                "raw_blob_path": subfolder,
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

    def test__historical_anonymisation__load_data__entraid_aiedocument_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        entity_name = "t_ha_ld_ead"
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

    def test__historical_anonymisation__load_data__service_bus(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        subfolder = "t_ha_ld_sb"
        entity_name = "t_ha_ld_sb__main"
        other_entity_name = "t_ha_ld_hb__other"
        raw_data_a = [
            {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_a,
            ("odw-raw", subfolder, "2025-01-01", entity_name, "raw_data_a.json"),
        )
        raw_data_b = [
            {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_b,
            ("odw-raw", subfolder, "2025-01-02", entity_name, "raw_data_b.json"),
        )
        raw_data_c = [
            {"id": 3, "name": "Sisko", "email": "bensisko@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_c,
            ("odw-raw", subfolder, "2025-01-01", entity_name, "raw_data_c.json"),
        )
        other_entity_data = [{"id": 1, "name": "Locutus", "email": "locutus@borg.com"}]
        self.write_json(
            other_entity_data,
            ("odw-raw", subfolder, "2025-01-01", other_entity_name, "raw_data_a.json"),
        )
        standardised_data_base_path = f"odw-standardised/sb_{entity_name}"
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
                {
                    "id": 3,
                    "name": "Sisko",
                    "email": "bensisko@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        standardised_data_b.write.format("parquet").mode("append").save(
            f"{warehouse_name}/{standardised_data_base_path}"
        )
        expected_raw_data = spark.createDataFrame(
            [
                {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
                {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
                {"id": 3, "name": "Sisko", "email": "bensisko@someorg.co.uk"},
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
                {
                    "id": 3,
                    "name": "Sisko",
                    "email": "bensisko@someorg.co.uk",
                    "layer": "standardised",
                },
            ]
        )
        override_config = {
            entity_name: {
                "raw_blob_path": subfolder,
                "raw_blob_format": "json",
                "standardised_blob_path": f"sb_{entity_name}",
                "category": "ServiceBus",
                "raw_blob_read_options": {"multiline": "true"},
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

    def test__historical_anonymisation__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity_name = "t_ha_ld_crd"
        override_config = {
            entity_name: {
                "raw_blob_path": entity_name,
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "Horizon",
                "raw_blob_read_options": {"header": "true"},
                "primary_keys": ["id"],
                "cols_to_revert_to_raw": ["email"],
            }
        }
        raw_data = spark.createDataFrame(
            [
                {"id": "1", "name": "Picard", "email": "myemail@someorg.co.uk"},
                {"id": "2", "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
            ]
        )
        standardised_data = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Picard",
                    "email": "myemail@someorg.co.uk",
                    "layer": "standardised",
                },
                {
                    "id": 2,
                    "name": "REDACTED",  # Should not be touched
                    "email": "o********************",  # An example entry with a malformed redaction, which should be overwritten
                    "layer": "standardised",
                },
            ]
        )
        source_data = {"standardised_data": standardised_data, "raw_data": raw_data}
        expected_data_to_anonymise = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Picard",
                    "email": "myemail@someorg.co.uk",
                    "layer": "standardised",
                },
                {
                    "id": 2,
                    "name": "REDACTED",  # Should not be touched
                    "email": "otherEmail@someorg.co.uk",  # Should be overwritten
                    "layer": "standardised",
                },
            ]
        )
        mock_anonymised_data = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "REDACTED",
                    "email": "REDACTED",
                    "layer": "standardised",
                },
                {
                    "id": 2,
                    "name": "REDACTED",
                    "email": "REDACTED",
                    "layer": "standardised",
                },
            ]
        )
        with (
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                return_value=mock_anonymised_data,
            ),
            mock.patch.object(
                HistoricalAnonymisationProcess, "_ENTITY_CONFIG", override_config
            ),
        ):
            data_to_write, etl_result = HistoricalAnonymisationProcess(
                spark=spark
            ).process(entity_name=entity_name, source_data=source_data)
            assert_dataframes_equal(
                mock_anonymised_data, data_to_write[entity_name]["data"]
            )
            assert_etl_result_successful(etl_result)
            data_that_was_anonymised = (
                AnonymisationEngine.apply_from_purview.call_args.args[0]
            )
            assert_dataframes_equal(
                expected_data_to_anonymise, data_that_was_anonymised
            )
