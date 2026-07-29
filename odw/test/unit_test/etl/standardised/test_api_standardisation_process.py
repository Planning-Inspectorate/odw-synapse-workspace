from unittest import mock
import json
import pyspark.sql.functions as F
from odw.core.etl.transformation.standardised.api_standardisation_process import (
    APIStandardisationProcess,
)
from odw.test.util.test_case import SparkTestCase
from odw.core.util.util import Util
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.util import (
    generate_local_path,
    get_all_file_names_in_directory,
)
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    LongType,
    StringType,
    TimestampType,
    IntegerType,
)
from typing import List, Any, Callable
import os
import pytest
import csv
from datetime import datetime
import hashlib


class TestAPIStandardisationAnonymisation(SparkTestCase):
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

    def generate_standardised_table_definitions(self):
        return {
            "fields": [
                {"metadata": {}, "name": "id", "type": "integer", "nullable": False},
                {
                    "metadata": {},
                    "name": "firstname",
                    "type": "string",
                    "nullable": False,
                },
                {
                    "metadata": {},
                    "name": "lastname",
                    "type": "string",
                    "nullable": False,
                },
            ]
        }

    def generate_raw_schema(self, file_format: str):
        id_col_type = LongType() if file_format == "json" else StringType()
        return StructType(
            [
                StructField("id", id_col_type, True),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
            ]
        )

    def test__api_standardisation_process__get_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = APIStandardisationProcess(spark)

        assert inst.get_name() == "API Standardisation Process"

    @pytest.mark.parametrize(
        "raw_data_write_format, raw_data_write_function",
        [("csv", write_csv), ("json", write_json)],
    )
    def test__api_standardisation_process__load_data(
        self, raw_data_write_format: str, raw_data_write_function: Callable
    ):
        """
        - Given I have some raw files (in json or csv format)
        - When I call load_data for a specific date folder
        - Then only the raw files for the specified date should be loaded, as well as the existing standardised table and orchestration data
        """
        entity_name = f"t_asp_ld_{raw_data_write_format}"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Create some raw data
        raw_schema = self.generate_raw_schema(raw_data_write_format)
        raw_data_a = [
            {"id": 1, "firstName": "Frodo", "lastName": "Baggins"},
            {"id": 2, "firstName": "Samwise", "lastName": "Gamgee"},
            {"id": 3, "firstName": "Merry", "lastName": "Brandybuck"},
        ]
        raw_data_a_df = spark.createDataFrame(raw_data_a, raw_schema)
        raw_data_write_function(
            self,
            raw_data_a,
            (
                "odw-raw",
                entity_name,
                "2025-01-01",
                f"{entity_name}_a.{raw_data_write_format}",
            ),
        )
        raw_data_b = [
            {"id": 4, "firstName": "Peregrin", "lastName": "Took"},
        ]
        raw_data_b_df = spark.createDataFrame(raw_data_b, raw_schema)
        raw_data_write_function(
            self,
            raw_data_b,
            (
                "odw-raw",
                entity_name,
                "2025-01-01",
                f"{entity_name}_b.{raw_data_write_format}",
            ),
        )
        # Create data for another date (which should be ignored by the APIStandardisationProcess)
        other_date_data = [
            {"id": 5, "firstName": "Aragorn", "lastName": "Son of Arathorn"},
            {"id": 6, "firstName": "Gandalf", "lastName": "The Grey"},
        ]
        raw_data_write_function(
            self,
            other_date_data,
            (
                "odw-raw",
                entity_name,
                "2024-12-31",
                f"{entity_name}.{raw_data_write_format}",
            ),
        )
        # Create the orchestration data
        orchestration_data_json = [
            {
                "Source_Filename_Start": entity_name,
                "Standardised_Table_Definition": f"standardised_table_definitions/{entity_name}/{entity_name}.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": entity_name,
                "Expected_Within_Weekdays": 1,
            }
        ]
        orchestration_data = spark.createDataFrame(
            [{"definitions": orchestration_data_json}],
            StructType(
                [
                    StructField(
                        "definitions",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "Expected_Within_Weekdays", LongType(), True
                                    ),
                                    StructField(
                                        "Source_Filename_Start", StringType(), True
                                    ),
                                    StructField(
                                        "Source_Frequency_Folder", StringType(), True
                                    ),
                                    StructField(
                                        "Standardised_Table_Definition",
                                        StringType(),
                                        True,
                                    ),
                                    StructField(
                                        "Standardised_Table_Name", StringType(), True
                                    ),
                                ]
                            ),
                            True,
                        ),
                        True,
                    )
                ]
            ),
        )
        # Create the standardised table definitions, which outlines column casting during processing
        standardised_table_definition = self.generate_standardised_table_definitions()
        standardised_table_definition_df = spark.createDataFrame(
            [{"value": json.dumps(standardised_table_definition)}],
            StructType([StructField("value", StringType(), True)]),
        )
        container_path = f"odw-raw/{entity_name}/2025-01-01"
        expected_loaded_data = {
            "definitions": orchestration_data_json,
            f"odw_standardised_db.{entity_name}": "read_source_data",
            f"{container_path}/{entity_name}_a.{raw_data_write_format}": raw_data_a_df,
            f"{container_path}/{entity_name}_a.{raw_data_write_format}_standardised_table_definition": standardised_table_definition,
            f"{container_path}/{entity_name}_b.{raw_data_write_format}": raw_data_b_df,
            f"{container_path}/{entity_name}_b.{raw_data_write_format}_standardised_table_definition": standardised_table_definition,
        }
        with (
            mock.patch.object(
                APIStandardisationProcess,
                "get_file_names_in_directory",
                get_all_file_names_in_directory,
            ),
            mock.patch.object(
                APIStandardisationProcess,
                "load_orchestration_data",
                return_value=orchestration_data,
            ),
            mock.patch.object(APIStandardisationProcess, "__init__", return_value=None),
            mock.patch.object(SynapseFileDataIO, "__init__", return_value=None),
            mock.patch.object(
                SynapseFileDataIO, "read", return_value=standardised_table_definition_df
            ),  # This is for reading the table definition json
            mock.patch.object(SynapseTableDataIO, "__init__", return_value=None),
            mock.patch.object(
                SynapseTableDataIO, "read", return_value="read_source_data"
            ),  # This is used just for reading the source data
            mock.patch.object(Util, "get_path_to_file", generate_local_path),
        ):
            inst = APIStandardisationProcess(spark)
            inst.spark = spark
            actual_loaded_data = inst.load_data(
                source_folder=entity_name, date_folder="2025-01-01"
            )
            assert set(actual_loaded_data.keys()) == set(expected_loaded_data.keys())
            raw_data_a_key = f"{container_path}/{entity_name}_a.{raw_data_write_format}"
            raw_data_b_key = f"{container_path}/{entity_name}_b.{raw_data_write_format}"
            actual_read_data_a = actual_loaded_data.pop(raw_data_a_key)
            actual_read_data_b = actual_loaded_data.pop(raw_data_b_key)
            assert_dataframes_equal(raw_data_a_df, actual_read_data_a)
            assert_dataframes_equal(raw_data_b_df, actual_read_data_b)
            expected_loaded_data.pop(raw_data_a_key)
            expected_loaded_data.pop(raw_data_b_key)
            assert expected_loaded_data == actual_loaded_data

    def test__api_standardisation_process__process(self):
        """
        - Given I have read some raw files, orchestration data and couldn't find an existing standardised table
        - When I call APIStandardisationProcess.process
        - Then metadata for two dataframe write operations should be returned, and a successful ETLResult should be returned
        """
        entity_name = "t_asp_p"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Create some raw data
        raw_schema = self.generate_raw_schema("json")
        raw_data_a = [
            {"id": 1, "firstName": "Frodo", "lastName": "Baggins"},
            {"id": 2, "firstName": "Samwise", "lastName": "Gamgee"},
            {"id": 3, "firstName": "Merry", "lastName": "Brandybuck"},
        ]
        raw_data_a_df = spark.createDataFrame(raw_data_a, raw_schema)
        raw_data_b = [
            {"id": 4, "firstName": "Peregrin", "lastName": "Took"},
        ]
        raw_data_b_df = spark.createDataFrame(raw_data_b, raw_schema)
        # Create a fake orchestration entry
        orchestration_data_json = [
            {
                "Source_Filename_Start": entity_name,
                "Standardised_Table_Definition": f"standardised_table_definitions/{entity_name}/{entity_name}.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": entity_name,
                "Expected_Within_Weekdays": 1,
            }
        ]
        # Create a fake standardised table definition (i.e. what the datatypes should be casted to)
        standardised_table_definition = self.generate_standardised_table_definitions()
        container_path = f"odw-raw/{entity_name}/2025-01-01"
        # Create the fake source data
        source_data = {
            "definitions": orchestration_data_json,
            f"odw_standardised_db.{entity_name}": None,
            f"{container_path}/{entity_name}_a.json": raw_data_a_df,
            f"{container_path}/{entity_name}_a.json_standardised_table_definition": standardised_table_definition,
            f"{container_path}/{entity_name}_b.json": raw_data_b_df,
            f"{container_path}/{entity_name}_b.json_standardised_table_definition": standardised_table_definition,
        }
        # Create the standardised data
        mock_storage_name = "mystorageaccount"
        standardised_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("firstname", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("ingested_datetime", TimestampType(), False),
                StructField("ingested_by_process_name", StringType(), False),
                StructField("expected_from", TimestampType(), False),
                StructField("expected_to", TimestampType(), False),
                StructField("input_file", StringType(), False),
                StructField("modified_datetime", TimestampType(), False),
                StructField("modified_by_process_name", StringType(), False),
                StructField("entity_name", StringType(), False),
                StructField("file_id", StringType(), True),
            ]
        )
        mock_process_name = "apistandardisationprocess"
        mock_input_file_name = "inputfilename"
        mock_current_timestamp = datetime(2025, 1, 1)
        standardised_cols = {
            "ingested_datetime": mock_current_timestamp,
            "ingested_by_process_name": mock_process_name,
            "expected_from": datetime(2024, 12, 31),
            "expected_to": mock_current_timestamp,
            "input_file": mock_input_file_name,
            "modified_datetime": mock_current_timestamp,
            "modified_by_process_name": mock_process_name,
            "entity_name": entity_name,
            "file_id": hashlib.sha256(
                f"{mock_input_file_name}{mock_current_timestamp}".encode("utf-8")
            ).hexdigest(),  # Can't compare because it depends on the current time
        }
        expected_standardised_a = spark.createDataFrame(
            [
                {"id": 1, "firstname": "Frodo", "lastname": "Baggins"}
                | standardised_cols,
                {"id": 2, "firstname": "Samwise", "lastname": "Gamgee"}
                | standardised_cols,
                {"id": 3, "firstname": "Merry", "lastname": "Brandybuck"}
                | standardised_cols,
            ],
            standardised_schema,
        )
        expected_standardised_b = spark.createDataFrame(
            [
                {"id": 4, "firstname": "Peregrin", "lastname": "Took"}
                | standardised_cols,
            ],
            standardised_schema,
        )
        # Define the expected output from the process() function
        expected_output = {
            f"odw_standardised_db.{entity_name}__1": {
                # data property removed for comparison - the data is compared directly
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_standardised_db",
                "table_name": entity_name,
                "storage_endpoint": mock_storage_name,
                "container_name": "odw-standardised",
                "blob_path": entity_name,
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": dict(),
            },
            f"odw_standardised_db.{entity_name}__2": {
                # data property removed for comparison - the data is compared directly
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_standardised_db",
                "table_name": entity_name,
                "storage_endpoint": mock_storage_name,
                "container_name": "odw-standardised",
                "blob_path": entity_name,
                "file_format": "delta",
                "write_mode": "append",
                "write_options": {"mergeSchema": "true"},
            },
        }
        with (
            mock.patch.object(APIStandardisationProcess, "__init__", return_value=None),
            mock.patch.object(
                APIStandardisationProcess, "get_name", return_value=mock_process_name
            ),
            mock.patch.object(
                F, "input_file_name", return_value=F.lit(mock_input_file_name)
            ),
            mock.patch.object(
                F, "current_timestamp", return_value=F.lit(mock_current_timestamp)
            ),
            mock.patch.object(
                F, "input_file_name", return_value=F.lit(mock_input_file_name)
            ),
            mock.patch.object(
                Util, "get_storage_account", return_value=mock_storage_name
            ),
            mock.patch.object(
                Util, "is_non_production_environment", return_value=False
            ),
        ):
            inst = APIStandardisationProcess()
            inst.spark = spark
            actual_output, etl_result = inst.process(
                source_data=source_data,
                source_folder=entity_name,
                date_folder="2025-01-01",
            )
            assert_etl_result_successful(etl_result)
            assert set(actual_output.keys()) == set(expected_output.keys())
            actual_standardised_a = actual_output[
                f"odw_standardised_db.{entity_name}__1"
            ].pop("data")
            actual_standardised_b = actual_output[
                f"odw_standardised_db.{entity_name}__2"
            ].pop("data")
            assert actual_output == expected_output
            assert_dataframes_equal(expected_standardised_a, actual_standardised_a)
            assert_dataframes_equal(expected_standardised_b, actual_standardised_b)
