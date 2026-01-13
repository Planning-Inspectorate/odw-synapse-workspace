from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path
from odw.test.util.util import get_all_files_in_directory, format_adls_path_to_local_path
from pyspark.sql import SparkSession
import mock
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import json
import pytest
import shutil
import os
from pathlib import Path
from typing import Dict, List, Any
import csv
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


TEST_DATA_FOLDER = "hznStdIntTest"


def add_orchestration_entries():
    orchestration_path = os.path.join("spark-warehouse", "odw-config", "orchestration", "orchestration.json")
    new_definitions = [
        {
            "Source_Filename_Start": "cases_specialisms",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": f"standardised_table_definitions/{TEST_DATA_FOLDER}/cases_specialisms.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "cases_specialisms",
            "Expected_Within_Weekdays": 1
        }
    ]
    with open(orchestration_path, "r") as f:
        existing_content = json.load(f)
    with open(orchestration_path, "w") as f:
        updated_content = {
            "definitions": existing_content["definitions"] + new_definitions
        }
        json.dump(updated_content, f, indent=4)


def write_csv(csv_data: List[List[Any]], path: List[str]):
    directories = path[:-1]
    os.makedirs(os.path.join("spark-warehouse", *directories), exist_ok=True)
    with open(os.path.join("spark-warehouse", *path), "w", newline="") as file:
        writer = csv.writer(file)
        # Write data to the CSV file
        writer.writerows(csv_data)


def write_json(json_data: Dict[str, Any], path: List[str]):
    directories = path[:-1]
    os.makedirs(os.path.join("spark-warehouse", *directories), exist_ok=True)
    with open(os.path.join("spark-warehouse", *path), "w", newline="") as file:
        json.dump(json_data, file, indent=4)


@pytest.fixture
def teardown(request: pytest.FixtureRequest):
    yield
    shutil.rmtree(
        generate_local_path(request.param),
        ignore_errors=True
    )


def compare_standardised_data(expected_df: DataFrame, actual_data: DataFrame):
    cols_to_ignore = ("ingested_datetime", "expected_from", "expected_to", "modified_datetime", "file_id")
    expected_df_cleaned = expected_df
    actual_data_cleaned = actual_data
    for col in cols_to_ignore:
        expected_df_cleaned = expected_df_cleaned.drop(col)
        actual_data_cleaned = actual_data_cleaned.drop(col)
    assert_dataframes_equal(expected_df_cleaned, actual_data_cleaned)


@pytest.mark.parametrize(
    "teardown",
    [
        os.path.join("odw-raw", TEST_DATA_FOLDER)
    ],
    indirect=["teardown"]
)
def test__horizon_standardisation_process__run__with_existing_data(teardown):
    # The ETL process depends on the orchestration.json file in the data lake. This ensures that the entries relevant for the
    # test have been added to this local file
    add_orchestration_entries()
    spark = SparkSession.builder.getOrCreate()
    # Global mock variables
    mock_current_datetime = datetime(2027, 1, 1)
    date_folder = "2027-02-01"
    # Create mock "new" data to add to the table
    raw_csv_data = (
        ("col_a", "col_b", "col_c"),
        ("a", "b", "c"),
        ("d", "e", "f"),
        ("g", "h", "i")
    )
    write_csv(raw_csv_data, ["odw-raw", TEST_DATA_FOLDER, date_folder, "cases_specialisms.csv"])
    # Create mock "existing" data which is already in the table
    existing_data = spark.createDataFrame(
        (
            ("j", "k", "l", datetime(2000, 1, 1), "horizon_standardisation_process", datetime(2000, 1, 1), datetime(2000, 1, 1), "some_input_file", datetime(2000, 1, 1), "horizon_standardisation_process", "cases_specialisms", "some_guid_a"),
            ("m", "n", "o", datetime(2026, 1, 1), "horizon_standardisation_process", datetime(2026, 1, 1), datetime(2026, 1, 1), "some_input_file", datetime(2026, 1, 1), "horizon_standardisation_process", "cases_specialisms", "some_guid_b")
        ),
        ("col_a", "col_b", "col_c", "ingested_datetime", "ingested_by_process_name", "expected_from", "expected_to", "input_file", "modified_datetime", "modified_by_process_name", "entity_name", "file_id")
    )
    with mock.patch.object(SynapseTableDataIO, "_format_to_adls_path", format_adls_path_to_local_path):
        SynapseTableDataIO().write(
            data=existing_data,
            spark=spark,
            database_name="odw_standardised_db",
            table_name="cases_specialisms",
            storage_name="blank",
            container_name="odw-standardised",
            blob_path="Horizon/cases_specialisms",
            file_format="delta",
            write_mode="overwrite"
        )
    # Create the standardised table definitions, which outlines column casting during processing
    standardised_table_definition = {
        "fields": [
            {
                "metadata": {},
                "name": "col_a",
                "type": "string",
                "nullable": False
            },
            {
                "metadata": {},
                "name": "col_b",
                "type": "string",
                "nullable": False
            },
            {
                "metadata": {},
                "name": "col_c",
                "type": "string",
                "nullable": False
            }
        ]
    }
    write_json(standardised_table_definition, ["odw-config", "standardised_table_definitions", TEST_DATA_FOLDER, "cases_specialisms.json"])

    # The expected final output after appending the new data to the existing data
    expected_table_data = spark.createDataFrame(
        (
            ("j", "k", "l", datetime(2000, 1, 1), "horizon_standardisation_process", datetime(2000, 1, 1), datetime(2000, 1, 1), "some_input_file", datetime(2000, 1, 1), "horizon_standardisation_process", "cases_specialisms", "some_guid_a"),
            ("m", "n", "o", datetime(2026, 1, 1), "horizon_standardisation_process", datetime(2026, 1, 1), datetime(2026, 1, 1), "some_input_file", datetime(2026, 1, 1), "horizon_standardisation_process", "cases_specialisms", "some_guid_b"),
            ("a", "b", "c", mock_current_datetime, "horizon_standardisation_process", mock_current_datetime, mock_current_datetime, "some_input_file", mock_current_datetime, "horizon_standardisation_process", "cases_specialisms", "some_guid_c"),
            ("d", "e", "f", mock_current_datetime, "horizon_standardisation_process", mock_current_datetime, mock_current_datetime, "some_input_file", mock_current_datetime, "horizon_standardisation_process", "cases_specialisms", "some_guid_d"),
            ("g", "h", "i", mock_current_datetime, "horizon_standardisation_process", mock_current_datetime, mock_current_datetime, "some_input_file", mock_current_datetime, "horizon_standardisation_process", "cases_specialisms", "some_guid_e")
        ),
        ("col_a", "col_b", "col_c", "ingested_datetime", "ingested_by_process_name", "expected_from", "expected_to", "input_file", "modified_datetime", "modified_by_process_name", "entity_name", "file_id")
    )
    # Run the full etl process
    with mock.patch.object(SynapseFileDataIO, "_format_to_adls_path", format_adls_path_to_local_path):
        with mock.patch.object(HorizonStandardisationProcess, "get_last_modified_folder", return_value=date_folder):
            with mock.patch.object(HorizonStandardisationProcess, "get_file_names_in_directory", return_value=["cases_specialisms.csv"]):
                mock_mssparkutils_context = {"pipelinejobid": "some_guid", "isForPipeline": True}
                with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                    with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
                        with mock.patch.object(Util, "get_path_to_file", generate_local_path):
                            with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
                                inst = HorizonStandardisationProcess(spark)
                                result = inst.run(source_folder=TEST_DATA_FOLDER)
                                assert_etl_result_successful(result)
                                actual_table_data = spark.table("odw_standardised_db.cases_specialisms")
                                compare_standardised_data(expected_table_data, actual_table_data)
