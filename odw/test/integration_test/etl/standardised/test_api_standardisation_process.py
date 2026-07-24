import mock
import pytest
from odw.test.util.assertion import (
    assert_etl_result_successful,
    assert_dataframes_equal,
)
import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.standardised.api_standardisation_process import (
    APIStandardisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.test.util.util import (
    format_adls_path_to_local_path,
    generate_local_path,
    get_all_file_names_in_directory,
    add_orchestration_entry,
)
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from typing import Dict, List, Any
from datetime import datetime
from contextlib import ExitStack
import os
import csv


def create_standardised_dataframe(
    data: List[Dict[str, Any]], schema: T.StructType
) -> DataFrame:
    standardised_cols = {
        "ingested_datetime": datetime(2025, 1, 1),
        "ingested_by_process_name": "API Standardisation Process",
        "expected_from": datetime(2024, 12, 31),
        "expected_to": datetime(2025, 1, 1),
        "input_file": "some_file",
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": "API Standardisation Process",
        "entity_name": "some_entity",
        "file_id": "1",
    }
    standardised_cols_schema = [
        T.StructField("ingested_datetime", T.TimestampType(), True),
        T.StructField("ingested_by_process_name", T.StringType(), True),
        T.StructField("expected_from", T.TimestampType(), True),
        T.StructField("expected_to", T.TimestampType(), True),
        T.StructField("input_file", T.StringType(), True),
        T.StructField("modified_datetime", T.TimestampType(), True),
        T.StructField("modified_by_process_name", T.StringType(), True),
        T.StructField("entity_name", T.StringType(), True),
        T.StructField("file_id", T.StringType(), True),
    ]
    enriched_data = [x | standardised_cols for x in data]
    spark = PytestSparkSessionUtil().get_spark_session()
    return spark.createDataFrame(
        enriched_data, schema=T.StructType(schema.fields + standardised_cols_schema)
    )


class TestAPIStandardisationProcess(ETLTestCase):
    @pytest.fixture(scope="session", autouse=True)
    def initialise_orchestration_file(self):
        new_definitions = [
            {
                "Source_Filename_Start": "t_asp_r_wned",
                "Standardised_Table_Definition": "standardised_table_definitions/t_asp_r_wned/t_asp_r_wned.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": "t_asp_r_wned",
                "Expected_Within_Weekdays": 1,
            },
            {
                "Source_Filename_Start": "t_asp_r_wed",
                "Standardised_Table_Definition": "standardised_table_definitions/t_asp_r_wed/t_asp_r_wed.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": "t_asp_r_wed",
                "Expected_Within_Weekdays": 1,
            },
        ]
        for definition in new_definitions:
            add_orchestration_entry(definition)

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
                APIStandardisationProcess,
                "get_file_names_in_directory",
                get_all_file_names_in_directory,
            ),
            mock.patch.object(Util, "get_path_to_file", generate_local_path),
            mock.patch.object(
                Util, "is_non_production_environment", return_value=False
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

    def compare_standardised_data(self, expected: DataFrame, actual: DataFrame):
        cols_to_ignore = [
            "ingested_datetime",
            "modified_datetime",
            "input_file",
            "entity_name",
            "file_id",
        ]
        expected_cleaned = expected.drop(*cols_to_ignore)
        actual_cleaned = actual.drop(*cols_to_ignore)
        assert_dataframes_equal(expected_cleaned, actual_cleaned)

    def test__api_standardisation_process__run__with_no_existing_data(self):
        """
        - Given I have a raw file ingested on 2025-01-01, and a raw file ingested on 2024-12-31, and there is no standardised data
        - When I run APIStandardisationProcess for 2025-01-01
        - Then the data for 2025-01-01 should be standardised, and the standardised table should be created
        """
        entity_name = "t_asp_r_wned"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Create some raw data
        raw_data = [
            {"id": 1, "firstName": "Frodo", "lastName": "Baggins"},
            {"id": 2, "firstName": "Samwise", "lastName": "Gamgee"},
            {"id": 3, "firstName": "Merry", "lastName": "Brandybuck"},
            {"id": 4, "firstName": "Peregrin", "lastName": "Took"},
        ]
        self.write_csv(
            raw_data,
            ("odw-raw", entity_name, "2025-01-01", f"{entity_name}.csv"),
        )
        # Create data for another date (which should be ignored by the APIStandardisationProcess)
        other_date_data = [
            {"id": 5, "firstName": "Aragorn", "lastName": "Son of Arathorn"},
            {"id": 6, "firstName": "Gandalf", "lastName": "The Grey"},
        ]
        self.write_csv(
            other_date_data,
            ("odw-raw", entity_name, "2024-12-31", f"{entity_name}.csv"),
        )
        # Create the standardised table definitions, which outlines column casting during processing
        standardised_table_definition = self.generate_standardised_table_definitions()
        self.write_json(
            standardised_table_definition,
            [
                "odw-config",
                "standardised_table_definitions",
                entity_name,
                f"{entity_name}.json",
            ],
        )
        expected_output_data = create_standardised_dataframe(
            [
                {"id": 1, "firstname": "Frodo", "lastname": "Baggins"},
                {"id": 2, "firstname": "Samwise", "lastname": "Gamgee"},
                {"id": 3, "firstname": "Merry", "lastname": "Brandybuck"},
                {"id": 4, "firstname": "Peregrin", "lastname": "Took"},
            ],
            T.StructType(
                [
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("firstname", T.StringType(), True),
                    T.StructField("lastname", T.StringType(), True),
                ]
            ),
        )
        etl_result = APIStandardisationProcess(
            spark,
        ).run(
            entity_name=entity_name,
            source_folder=entity_name,
            date_folder="2025-01-01",
            orchestration_run_id=entity_name,
            orchestration_entity_name=entity_name,
            orchestration_stage_name="historical_anonymisation",
        )
        assert_etl_result_successful(etl_result)
        actual_output_data = spark.table(f"odw_standardised_db.{entity_name}")
        self.compare_standardised_data(expected_output_data, actual_output_data)

    def test__api_standardisation_process__run__with_existing_data(self):
        """
        - Given I have a raw file ingested on 2025-01-01, and a raw file ingested on 2024-12-31, and there is an existing standardised table
        - When I run APIStandardisationProcess for 2025-01-01
        - Then the data for 2025-01-01 should be standardised, and the standardised table should be created
        """
        entity_name = "t_asp_r_wed"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Create some raw data
        raw_data = [
            {"id": 1, "firstName": "Frodo", "lastName": "Baggins"},
            {"id": 2, "firstName": "Samwise", "lastName": "Gamgee"},
            {"id": 3, "firstName": "Merry", "lastName": "Brandybuck"},
            {"id": 4, "firstName": "Peregrin", "lastName": "Took"},
        ]
        self.write_csv(
            raw_data,
            ("odw-raw", entity_name, "2025-01-01", f"{entity_name}.csv"),
        )
        # Create data for another date (which should be ignored by the APIStandardisationProcess)
        other_date_data = [
            {"id": 5, "firstName": "Aragorn", "lastName": "Son of Arathorn"},
            {"id": 6, "firstName": "Gandalf", "lastName": "The Grey"},
        ]
        self.write_csv(
            other_date_data,
            ("odw-raw", entity_name, "2024-12-31", f"{entity_name}.csv"),
        )
        existing_standardised_data = create_standardised_dataframe(
            [
                {"id": 10, "firstname": "Witchking", "lastname": "Of Angmar"},
            ],
            T.StructType(
                [
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("firstname", T.StringType(), True),
                    T.StructField("lastname", T.StringType(), True),
                ]
            ),
        )
        self.write_existing_table(
            spark,
            existing_standardised_data,
            entity_name,
            "odw_standardised_db",
            "odw-standardised",
            entity_name,
            "overwrite",
        )
        # Create the standardised table definitions, which outlines column casting during processing
        standardised_table_definition = self.generate_standardised_table_definitions()
        self.write_json(
            standardised_table_definition,
            [
                "odw-config",
                "standardised_table_definitions",
                entity_name,
                f"{entity_name}.json",
            ],
        )
        expected_output_data = create_standardised_dataframe(
            [
                {"id": 1, "firstname": "Frodo", "lastname": "Baggins"},
                {"id": 2, "firstname": "Samwise", "lastname": "Gamgee"},
                {"id": 3, "firstname": "Merry", "lastname": "Brandybuck"},
                {"id": 4, "firstname": "Peregrin", "lastname": "Took"},
                {
                    "id": 10,
                    "firstname": "Witchking",
                    "lastname": "Of Angmar",
                },  # Row from pre-existing table should be appended to
            ],
            T.StructType(
                [
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("firstname", T.StringType(), True),
                    T.StructField("lastname", T.StringType(), True),
                ]
            ),
        )
        etl_result = APIStandardisationProcess(
            spark,
        ).run(
            entity_name=entity_name,
            source_folder=entity_name,
            date_folder="2025-01-01",
            orchestration_run_id=entity_name,
            orchestration_entity_name=entity_name,
            orchestration_stage_name="historical_anonymisation",
        )
        assert_etl_result_successful(etl_result)
        actual_output_data = spark.table(f"odw_standardised_db.{entity_name}")
        self.compare_standardised_data(expected_output_data, actual_output_data)
