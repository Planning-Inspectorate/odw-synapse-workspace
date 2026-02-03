from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path
from odw.test.util.util import format_adls_path_to_local_path, format_to_adls_path
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
import pyspark.sql.types as T
import logging


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    with mock.patch("notebookutils.mssparkutils.runtime.context", {"pipelinejobid": "some_guid", "isForPipeline": True}):
        with mock.patch.object(SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                with mock.patch.object(Util, "get_path_to_file", generate_local_path):
                    with mock.patch.object(LoggingUtil, "__new__"):
                        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                            with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                                yield


def add_orchestration_entries():
    orchestration_path = os.path.join("spark-warehouse", "odw-config", "orchestration", "orchestration.json")
    new_definitions = [
        {
            "Source_Filename_Start": "test_hzn_std_pc_exst_data",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": f"standardised_table_definitions/test_hzn_std_pc_exst_data/test_hzn_std_pc_exst_data.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "test_hzn_std_pc_exst_data",
            "Expected_Within_Weekdays": 1,
        },
        {
            "Source_Filename_Start": "test_hzn_std_pc_no_exst_data",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": f"standardised_table_definitions/test_hzn_std_pc_no_exst_data/test_hzn_std_pc_no_exst_data.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "test_hzn_std_pc_no_exst_data",
            "Expected_Within_Weekdays": 1,
        },
    ]
    with open(orchestration_path, "r") as f:
        existing_content = json.load(f)
    with open(orchestration_path, "w") as f:
        updated_content = {"definitions": existing_content["definitions"] + new_definitions}
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


def generate_standardised_table_definitions():
    return {
        "fields": [
            {"metadata": {}, "name": "col_a", "type": "string", "nullable": False},
            {"metadata": {}, "name": "col_b", "type": "string", "nullable": False},
            {"metadata": {}, "name": "col_c", "type": "string", "nullable": False},
        ]
    }


def generate_output_table_schema():
    # Note: delta tables do not support non-nullable columns
    return T.StructType(
        [
            T.StructField("col_a", T.StringType(), True),
            T.StructField("col_b", T.StringType(), True),
            T.StructField("col_c", T.StringType(), True),
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
    )


def write_existing_table(data: DataFrame, table_name: str, database_name: str, container: str, blob_path: str):
    logging.info(f"Createing table '{database_name}.{table_name}'")
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
    table_path = f"{database_name}.{table_name}"
    data_path = format_to_adls_path(None, container, blob_path)
    writer = data.write.format("delta").mode("overwrite").option("path", data_path)
    writer.saveAsTable(table_path)


@pytest.fixture
def teardown(request: pytest.FixtureRequest):
    yield
    for table in request.param:
        shutil.rmtree(generate_local_path(table), ignore_errors=True)


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
    [[os.path.join("odw-raw", "test_hzn_std_pc_exst_data"), os.path.join("odw-standardised", "Horizon", "test_hzn_std_pc_exst_data")]],
    indirect=["teardown"],
)
def test__horizon_standardisation_process__run__with_existing_data(teardown):
    data_folder = "test_hzn_std_pc_exst_data"
    # The ETL process depends on the orchestration.json file in the data lake. This ensures that the entries relevant for the
    # test have been added to this local file
    add_orchestration_entries()
    spark = SparkSession.builder.getOrCreate()
    # Global mock variables
    mock_current_datetime = datetime(2027, 1, 1)
    date_folder = "2027-02-01"
    # Create mock "new" data to add to the table
    raw_csv_data = (("col_a", "col_b", "col_c"), ("a", "b", "c"), ("d", "e", "f"), ("g", "h", "i"))
    write_csv(raw_csv_data, ["odw-raw", data_folder, date_folder, "test_hzn_std_pc_exst_data.csv"])
    # Create mock "existing" data which is already in the table
    existing_data = spark.createDataFrame(
        (
            (
                "j",
                "k",
                "l",
                datetime(2000, 1, 1),
                "horizon_standardisation_process",
                datetime(2000, 1, 1),
                datetime(2000, 1, 1),
                "some_input_file",
                datetime(2000, 1, 1),
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_a",
            ),
            (
                "m",
                "n",
                "o",
                datetime(2026, 1, 1),
                "horizon_standardisation_process",
                datetime(2026, 1, 1),
                datetime(2026, 1, 1),
                "some_input_file",
                datetime(2026, 1, 1),
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_b",
            ),
        ),
        generate_output_table_schema(),
    )
    write_existing_table(
        existing_data,
        "test_hzn_std_pc_exst_data",
        "odw_standardised_db",
        "odw-standardised",
        "Horizon/test_hzn_std_pc_exst_data"
    )
    # Create the standardised table definitions, which outlines column casting during processing
    standardised_table_definition = generate_standardised_table_definitions()
    write_json(standardised_table_definition, ["odw-config", "standardised_table_definitions", data_folder, "test_hzn_std_pc_exst_data.json"])

    # The expected final output after appending the new data to the existing data
    expected_table_data = spark.createDataFrame(
        (
            (
                "j",
                "k",
                "l",
                datetime(2000, 1, 1),
                "horizon_standardisation_process",
                datetime(2000, 1, 1),
                datetime(2000, 1, 1),
                "some_input_file",
                datetime(2000, 1, 1),
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_a",
            ),
            (
                "m",
                "n",
                "o",
                datetime(2026, 1, 1),
                "horizon_standardisation_process",
                datetime(2026, 1, 1),
                datetime(2026, 1, 1),
                "some_input_file",
                datetime(2026, 1, 1),
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_b",
            ),
            (
                "a",
                "b",
                "c",
                mock_current_datetime,
                "horizon_standardisation_process",
                mock_current_datetime,
                mock_current_datetime,
                "some_input_file",
                mock_current_datetime,
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_c",
            ),
            (
                "d",
                "e",
                "f",
                mock_current_datetime,
                "horizon_standardisation_process",
                mock_current_datetime,
                mock_current_datetime,
                "some_input_file",
                mock_current_datetime,
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_d",
            ),
            (
                "g",
                "h",
                "i",
                mock_current_datetime,
                "horizon_standardisation_process",
                mock_current_datetime,
                mock_current_datetime,
                "some_input_file",
                mock_current_datetime,
                "horizon_standardisation_process",
                "test_hzn_std_pc_exst_data",
                "some_guid_e",
            ),
        ),
        generate_output_table_schema(),
    )
    # Run the full etl process
    with mock.patch.object(HorizonStandardisationProcess, "get_last_modified_folder", return_value=date_folder):
        with mock.patch.object(HorizonStandardisationProcess, "get_file_names_in_directory", return_value=["test_hzn_std_pc_exst_data.csv"]):
            with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
                inst = HorizonStandardisationProcess(spark)
                result = inst.run(source_folder=data_folder)
                assert_etl_result_successful(result)
                actual_table_data = spark.table("odw_standardised_db.test_hzn_std_pc_exst_data")
                print("final table nullable: ", actual_table_data.schema["col_a"].nullable)
                compare_standardised_data(expected_table_data, actual_table_data)


@pytest.mark.parametrize(
    "teardown",
    [[os.path.join("odw-raw", "test_hzn_std_pc_no_exst_data"), os.path.join("odw-standardised", "Horizon", "test_hzn_std_pc_no_exst_data")]],
    indirect=["teardown"],
)
def test__horizon_standardisation_process__run__with_no_existing_data(teardown):
    data_folder = "test_hzn_std_pc_no_exst_data"
    # The ETL process depends on the orchestration.json file in the data lake. This ensures that the entries relevant for the
    # test have been added to this local file
    add_orchestration_entries()
    spark = SparkSession.builder.getOrCreate()
    # Global mock variables
    mock_current_datetime = datetime(2027, 1, 1)
    date_folder = "2027-02-01"
    # Create mock "new" data to add to the table
    raw_csv_data = (("col_a", "col_b", "col_c"), ("a", "b", "c"), ("d", "e", "f"), ("g", "h", "i"))
    write_csv(raw_csv_data, ["odw-raw", data_folder, date_folder, "test_hzn_std_pc_no_exst_data.csv"])
    # Create the standardised table definitions, which outlines column casting during processing
    standardised_table_definition = generate_standardised_table_definitions()
    write_json(standardised_table_definition, ["odw-config", "standardised_table_definitions", data_folder, "test_hzn_std_pc_no_exst_data.json"])

    spark.sql("DROP TABLE IF EXISTS odw_standardised_db.test_hzn_std_pc_no_exst_data")

    # The expected final output after appending the new data to the existing data
    common_elements = (
        mock_current_datetime,
        "horizon_standardisation_process",
        mock_current_datetime,
        mock_current_datetime,
        "some_input_file",
        mock_current_datetime,
        "horizon_standardisation_process",
        "test_hzn_std_pc_no_exst_data",
    )
    expected_table_data = spark.createDataFrame(
        (
            ("a", "b", "c") + common_elements + ("some_guid_c",),
            ("d", "e","f") + common_elements + ("some_guid_d",),
            ("g", "h", "i") + common_elements + ("some_guid_e",)
        ),
        generate_output_table_schema(),
    )
    # Run the full etl process
    with mock.patch.object(HorizonStandardisationProcess, "get_last_modified_folder", return_value=date_folder):
        with mock.patch.object(
            HorizonStandardisationProcess, "get_file_names_in_directory", return_value=["test_hzn_std_pc_no_exst_data.csv"]
        ):
            with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
                inst = HorizonStandardisationProcess(spark)
                result = inst.run(source_folder=data_folder)
                assert_etl_result_successful(result)
                actual_table_data = spark.table("odw_standardised_db.test_hzn_std_pc_no_exst_data")
                print("final table nullable: ", actual_table_data.schema["col_a"].nullable)
                compare_standardised_data(expected_table_data, actual_table_data)
