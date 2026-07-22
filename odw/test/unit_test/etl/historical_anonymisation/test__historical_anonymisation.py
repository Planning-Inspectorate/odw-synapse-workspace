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
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.anonymisation.engine import AnonymisationEngine
from pyspark.sql import DataFrame
from typing import List, Any
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
        """
        Tests that raw data spread over many files for a specific entity are loaded correctly (For horizon data in csv format)

        - Given I have a horizon entity in a folder structure similar to the raw horizon data, and similar data for the standardised layer
        - When I call HistoricalAnonymisationProcess.load_data
        - Then them only the raw and standardised data for the specific entity should be read
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        subfolder = "t_ha_ld_hd"  # For the "real" data this will be set to "Horizon" - this is set differently here to prevent conflicts
        entity_name = "t_ha_ld_hd__main"
        other_entity_name = "t_ha_ld_hd__other"
        # Create some csv files that represent the historical horizon data
        raw_data_a = [
            {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_a,
            ("odw-raw", subfolder, "2025-01-01", f"{entity_name}_a.csv"),
        )
        raw_data_b = [
            {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
        ]
        self.write_csv(
            raw_data_b,
            ("odw-raw", subfolder, "2025-01-02", f"{entity_name}_b.csv"),
        )
        # Create a csv file for a different horizon entity - this one shouldn't be loaded by the ETL process
        other_entity_data = [{"id": 1, "name": "Locutus", "email": "locutus@borg.com"}]
        self.write_csv(
            other_entity_data,
            ("odw-raw", subfolder, "2025-01-01", f"{other_entity_name}_a.csv"),
        )
        # Create standardised data that aligns with the corresponding raw data
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
        # Create standardised data for the "other" entity which should be ignored by the ETLProcess
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
        # Expect the full raw/standardised data to be loaded (with the "other" entity excluded)
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
                "raw_blob_path": subfolder,  # For the "real" data this will be set to "Horizon" - this is set differently here to prevent conflicts
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "Horizon",
                "horizon_file_name": entity_name,
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
        """
        Tests that raw data spread over many files for a specific entity are loaded correctly (for entraid and aiedocument data)

        - Given I have an entity with raw data in a similar structure to the raw entraid and aiedocument data, and some similar standardised data
        - When I call HistoricalAnonymisationProcess.load_data
        - Then them only the raw and standardised data for the specific entity should be read
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        entity_name = "t_ha_ld_ead"
        raw_data_base_path = ("odw-raw", entity_name)
        # Create the raw data
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
        # Create the standardised data

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
        # Expect the raw and standardised data to be loaded
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
                "raw_blob_file_name_starts_with": "raw_data",
                "standardised_blob_path": entity_name,
                "category": "entraid",
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
        """
        Tests that raw data spread over many files for a specific entity are loaded correctly (For service bus data)

        - Given I have a service bus entity in a folder structure similar to the raw horizon data, and similar data for the standardised layer
        - When I call HistoricalAnonymisationProcess.load_data
        - Then them only the raw and standardised data for the specific entity should be read
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        subfolder = "t_ha_ld_sb"  # For the "real" data this will be set to "ServiceBus" - this is set differently here to prevent conflicts
        entity_name = "t_ha_ld_sb__main"
        other_entity_name = "t_ha_ld_hb__other"
        # Create some raw data for the service bus entity
        raw_data_a = [
            {"id": 1, "name": "Picard", "email": "myemail@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_a,
            ("odw-raw", subfolder, entity_name, "2025-01-01", f"{entity_name}_a.json"),
        )
        raw_data_b = [
            {"id": 2, "name": "Janeway", "email": "otherEmail@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_b,
            ("odw-raw", subfolder, entity_name, "2025-01-02", f"{entity_name}_b.json"),
        )
        # Can have many files in the same folder
        raw_data_c = [
            {"id": 3, "name": "Sisko", "email": "bensisko@someorg.co.uk"},
        ]
        self.write_json(
            raw_data_c,
            ("odw-raw", subfolder, entity_name, "2025-01-01", f"{entity_name}_c.json"),
        )
        # Create a separate entity, which shouldn't be loaded by the ETLProcess
        other_entity_data = [{"id": 1, "name": "Locutus", "email": "locutus@borg.com"}]
        self.write_json(
            other_entity_data,
            (
                "odw-raw",
                subfolder,
                other_entity_name,
                "2025-01-01",
                f"{other_entity_name}_a.json",
            ),
        )
        # Create standardised data
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
        # Expect all of the raw and standardised data to be loaded
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
                "raw_blob_path": subfolder,  # For the "real" data this will be set to "ServiceBus" - this is set differently here to prevent conflicts
                "raw_blob_format": "json",
                "standardised_blob_path": f"sb_{entity_name}",
                "category": "ServiceBus",
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
                "primary_keys": ["id"],
                "cols_to_revert_to_raw": ["email"],
                "horizon_file_name": entity_name,
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

    def test__historical_anonymisation__validate_all_anonymised_data__with_invalid_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity_name = "t_ha_vaad_wir"
        override_config = {
            entity_name: {
                "raw_blob_path": entity_name,
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "ServiceBus",
                "primary_keys": [
                    "id",
                    "minionCount",
                ],  # minionCount should be ignored from the comparison because it is not a string
                "cols_to_revert_to_raw": ["name"],
                "horizon_file_name": entity_name,
            }
        }
        standardised_data = spark.createDataFrame(
            [
                {"id": 1, "name": "Walter White", "job": "Teacher", "minionCount": 1},
                {
                    "id": 2,
                    "name": "j***********",  # Anonymised with bad format
                    "job": "Lawyer",
                    "minionCount": 2,
                },
            ]
        )
        expected_filtered_data = spark.createDataFrame(
            [
                {
                    "id": 2,
                    "name": "j***********",  # Anonymised with bad format
                    "job": "Lawyer",
                    "minionCount": 2,
                }
            ]
        )
        with (
            mock.patch.object(
                HistoricalAnonymisationProcess, "_ENTITY_CONFIG", override_config
            ),
            mock.patch.object(
                SynapseFileDataIO, "read", return_value=standardised_data
            ),
            mock.patch.object(Util, "display_dataframe", return_value=None),
        ):
            HistoricalAnonymisationProcess(spark).validate_all_anonymised_data(
                entity_name
            )
            actual_filtered_data = Util.display_dataframe.call_args.args[0]
            assert_dataframes_equal(expected_filtered_data, actual_filtered_data)

    def test__historical_anonymisation__validate_all_anonymised_data__with_all_valid_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity_name = "t_ha_vaad_wavr"
        override_config = {
            entity_name: {
                "raw_blob_path": entity_name,
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "ServiceBus",
                "primary_keys": [
                    "id",
                    "minionCount",
                ],  # minionCount should be ignored from the comparison because it is not a string
                "cols_to_revert_to_raw": ["name"],
                "horizon_file_name": entity_name,
            }
        }
        standardised_data = spark.createDataFrame(
            [
                {"id": 1, "name": "Walter White", "job": "Teacher", "minionCount": 1},
                {
                    "id": 2,
                    "name": "Jimmy Mcgill",  # Anonymised with bad format
                    "job": "Lawyer",
                    "minionCount": 2,
                },
            ]
        )
        with (
            mock.patch.object(
                HistoricalAnonymisationProcess, "_ENTITY_CONFIG", override_config
            ),
            mock.patch.object(
                SynapseFileDataIO, "read", return_value=standardised_data
            ),
            mock.patch.object(Util, "display_dataframe", return_value=None),
        ):
            HistoricalAnonymisationProcess(spark).validate_all_anonymised_data(
                entity_name
            )
            assert not Util.display_dataframe.called
