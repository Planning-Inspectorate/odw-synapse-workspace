from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path, format_adls_path_to_local_path, format_to_adls_path, add_orchestration_entries
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from pyspark.sql import SparkSession
import mock
import pytest
import shutil
import os
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


@pytest.fixture
def teardown(request: pytest.FixtureRequest):
    yield
    for table in request.param:
        shutil.rmtree(generate_local_path(table), ignore_errors=True)


def generate_standardised_table_schema(base_schema: T.StructType):
    # Note: delta tables do not support non-nullable columns
    return T.StructType(
        base_schema.fields
        + [
            T.StructField("message_id", T.StringType()),
            T.StructField("ingested_datetime", T.TimestampType()),
            T.StructField("expected_from", T.TimestampType()),
            T.StructField("expected_to", T.TimestampType()),
            T.StructField("message_type", T.StringType()),
            T.StructField("message_enqueued_time_utc", T.StringType()),
            T.StructField("input_file", T.StringType()),
        ]
    )


def generate_harmonised_table_schema(base_schema: T.StructType, incremental_key_col: str):
    # Note: delta tables do not support non-nullable columns
    return T.StructType(
        base_schema.fields
        + [
            T.StructField("message_id", T.StringType()),
            T.StructField(incremental_key_col, T.LongType()),
            T.StructField("migrated", T.StringType()),
            T.StructField("ODTSourceSystem", T.StringType()),
            T.StructField("SourceSystemID", T.StringType()),
            T.StructField("IngestionDate", T.StringType()),
            T.StructField("ValidTo", T.StringType()),
            T.StructField("IsActive", T.StringType()),
            T.StructField("RowID", T.StringType()),
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


def compare_harmonised_data(expected_df: DataFrame, actual_data: DataFrame):
    cols_to_ignore = ("incremental_key",)
    expected_df_cleaned = expected_df
    actual_data_cleaned = actual_data
    for col in cols_to_ignore:
        expected_df_cleaned = expected_df_cleaned.drop(col)
        actual_data_cleaned = actual_data_cleaned.drop(col)
    assert_dataframes_equal(expected_df_cleaned, actual_data_cleaned)


@pytest.mark.parametrize(
    "teardown",
    [
        [
            os.path.join("odw-standardised", "test_sb_hrm_pc_exst_data"),
            os.path.join("odw_harmonised_db.db", "test_sb_hrm_pc_exst_data"),
            os.path.join("odw-harmonised", "test_sb_hrm_pc_exst_data"),
        ]
    ],
    indirect=["teardown"],
)
def test__service_bus_harmonisation_process__run__with_existing_data_same_schema(teardown):
    """
    - Given I already have a harmonsed delta table, and I have some modified and deleted rows defined in the standardised layer
    - When I call ServiceBusHarmonisationProcess.run
    - Then the modified rows should be updated, and removed rows should be deleted from the harmonised table
    """
    add_orchestration_entries(
        {
            "Source_Filename_Start": "test_sb_hrm_pc_exst_data",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": "standardised_table_definitions/test_sb_hrm_pc_exst_data/test_sb_hrm_pc_exst_data.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "test_sb_hrm_pc_exst_data",
            "Expected_Within_Weekdays": 1,
            "Harmonised_Table_Name": "test_sb_hrm_pc_exst_data",
            "Harmonised_Incremental_Key": "incremental_key",
            "Entity_Primary_Key": "col_a",
        }
    )
    spark = SparkSession.builder.getOrCreate()
    table_name = "test_sb_hrm_pc_exst_data"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    incremental_key = "incremental_key"
    base_schema = T.StructType(
        [T.StructField("col_a", T.StringType()), T.StructField("col_b", T.StringType()), T.StructField("col_c", T.StringType())]
    )
    existing_data_ingestion_date_string = "2025-09-12T10:30:59.405000+0000"
    # Create existing harmonised table
    existing_harmonised_data = spark.createDataFrame(
        (
            ("a", "b", "c", "id1", 1, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("d", "e", "f", "id2", 2, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("e", "f", "g", "id3", 3, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("p", "q", "r", "id4", 4, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),  # Will be updated
            ("x", "y", "z", "id5", 5, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),  # Will be deleted
        ),
        generate_harmonised_table_schema(base_schema, incremental_key),
    )
    write_existing_table(existing_harmonised_data, table_name, "odw_harmonised_db", "odw-harmonised", table_name)

    # Create standardised table
    existing_data_ingestion_date = datetime.strptime(existing_data_ingestion_date_string, datetime_format)
    input_file = "some_file"
    already_existing_standardised_col_data = (
        existing_data_ingestion_date,
        existing_data_ingestion_date,
        existing_data_ingestion_date,
        "Create",
        existing_data_ingestion_date_string,
        input_file,
    )
    current_time = datetime.now()
    current_time_string = current_time.strftime(datetime_format)
    created_standardised_col_data = (current_time, current_time, current_time, "Create", current_time_string, input_file)
    updated_standardised_col_data = (current_time, current_time, current_time, "Update", current_time_string, input_file)
    deleted_standardised_col_data = (current_time, current_time, current_time, "Delete", current_time_string, input_file)
    standardised_data = spark.createDataFrame(
        (
            ("a", "b", "c", "id1") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("d", "e", "f", "id2") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("e", "f", "g", "id3") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("p", "q", "r", "id4") + already_existing_standardised_col_data,  # Already in the harmonised table (but will be updated)
            ("x", "y", "z", "id5") + already_existing_standardised_col_data,  # Already in the harmonised table (but will be deleted)
            ("g", "h", "i", "id6") + created_standardised_col_data,  # New data to add to the harmonised table
            ("j", "k", "l", "id7") + created_standardised_col_data,  # New data to add to the harmonised table
            ("m", "n", "o", "id8") + created_standardised_col_data,  # New data to add to the harmonised table
            ("p", "q", "s", "id9") + updated_standardised_col_data,  # A column to be updated, "col_c" updated from 'r' -> 's'
            ("x", "y", "z", "id10") + deleted_standardised_col_data,  # A column to be deleted
        ),
        generate_standardised_table_schema(base_schema),
    )
    write_existing_table(standardised_data, table_name, "odw_standardised_db", "odw-standardised", table_name)

    expected_harmonised_data_after_writing = spark.createDataFrame(
        (
            ("a", "b", "c", "id1", 1, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("d", "e", "f", "id2", 2, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("e", "f", "g", "id3", 3, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("p", "q", "r", "id4", 4, "1", "ODT", 1, existing_data_ingestion_date_string, current_time_string, "N", ""),  # Will be updated
            (
                "x",
                "y",
                "z",
                "id5",
                5,
                "1",
                "ODT",
                1,
                existing_data_ingestion_date_string,
                current_time_string,
                "N",
                "",
            ),  # Deleted (but left in history)
            ("p", "q", "s", "id9", 5, "1", "ODT", 1, current_time_string, "", "Y", ""),  # Should be updated
            ("g", "h", "i", "id6") + (6, "1", "ODT", 1, current_time_string, "", "Y", ""),  # New row should be added
            ("j", "k", "l", "id7") + (7, "1", "ODT", 1, current_time_string, "", "Y", ""),  # New row should be added
            ("m", "n", "o", "id8") + (8, "1", "ODT", 1, current_time_string, "", "Y", ""),  # New row should be added
        ),
        generate_harmonised_table_schema(base_schema, incremental_key),
    )
    # Run the full etl process
    with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
        with mock.patch.object(Util, "display_dataframe"):
            inst = ServiceBusHarmonisationProcess(spark)
            result = inst.run(entity_name="test_sb_hrm_pc_exst_data")
            assert_etl_result_successful(result)
            actual_table_data = spark.table("odw_harmonised_db.test_sb_hrm_pc_exst_data")
            compare_harmonised_data(expected_harmonised_data_after_writing, actual_table_data)


@pytest.mark.parametrize(
    "teardown",
    [
        [
            os.path.join("odw-standardised", "test_sb_hrm_pc_chg_schema"),
            os.path.join("odw_harmonised_db.db", "test_sb_hrm_pc_chg_schema"),
            os.path.join("odw-harmonised", "test_sb_hrm_pc_chg_schema"),
        ]
    ],
    indirect=["teardown"],
)
def test__service_bus_harmonisation_process__run__with_existing_data_different_schema(teardown):
    """
    - Given I already have a harmonsed delta table, and I the existing data in the standardised layer has new columns added
    - When I call ServiceBusHarmonisationProcess.run
    - Then the new columns should be added to the harmonised table
    """
    add_orchestration_entries(
        {
            "Source_Filename_Start": "test_sb_hrm_pc_chg_schema",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": "standardised_table_definitions/test_sb_hrm_pc_chg_schema/test_sb_hrm_pc_chg_schema.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "test_sb_hrm_pc_chg_schema",
            "Expected_Within_Weekdays": 1,
            "Harmonised_Table_Name": "test_sb_hrm_pc_chg_schema",
            "Harmonised_Incremental_Key": "incremental_key",
            "Entity_Primary_Key": "col_a",
        }
    )
    spark = SparkSession.builder.getOrCreate()
    table_name = "test_sb_hrm_pc_chg_schema"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    incremental_key = "incremental_key"
    base_schema = T.StructType(
        [T.StructField("col_a", T.StringType()), T.StructField("col_b", T.StringType()), T.StructField("col_c", T.StringType())]
    )
    altered_schema = T.StructType(
        [
            T.StructField("col_a", T.StringType()),
            T.StructField("col_b", T.StringType()),
            T.StructField("col_c", T.StringType()),
            T.StructField("col_d", T.StringType()),
        ]
    )
    existing_data_ingestion_date_string = "2025-09-12T10:30:59.405000+0000"
    # Create existing harmonised table
    existing_harmonised_data = spark.createDataFrame(
        (
            ("a", "b", "c", "id1", 1, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("d", "e", "f", "id2", 2, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("e", "f", "g", "id3", 3, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
            ("p", "q", "r", "id4", 4, "1", "ODT", 1, existing_data_ingestion_date_string, None, "Y", ""),
        ),
        generate_harmonised_table_schema(base_schema, incremental_key),
    )
    write_existing_table(existing_harmonised_data, table_name, "odw_harmonised_db", "odw-harmonised", table_name)

    # Create standardised table
    input_file = "some_file"
    current_time = datetime.now()
    current_time_string = current_time.strftime(datetime_format)
    already_existing_standardised_col_data = (
        current_time,
        current_time,
        current_time,
        "Update",
        current_time_string,
        input_file,
    )
    standardised_data = spark.createDataFrame(
        (
            ("a", "b", "c", "sisko", "id5") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("d", "e", "f", "worf", "id6") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("e", "f", "g", "kira", "id7") + already_existing_standardised_col_data,  # Already in the harmonised table
            ("p", "q", "r", "odo", "id8") + already_existing_standardised_col_data,  # Already in the harmonised table
        ),
        generate_standardised_table_schema(altered_schema),
    )
    write_existing_table(standardised_data, table_name, "odw_standardised_db", "odw-standardised", table_name)

    expected_harmonised_data_after_writing = spark.createDataFrame(
        (
            ("a", "b", "c", None, "id1", 1, "1", "ODT", 1, existing_data_ingestion_date_string, current_time_string, "N", ""),
            ("d", "e", "f", None, "id2", 2, "1", "ODT", 1, existing_data_ingestion_date_string, current_time_string, "N", ""),
            ("e", "f", "g", None, "id3", 3, "1", "ODT", 1, existing_data_ingestion_date_string, current_time_string, "N", ""),
            ("p", "q", "r", None, "id4", 4, "1", "ODT", 1, existing_data_ingestion_date_string, current_time_string, "N", ""),
            ("a", "b", "c", "sisko", "id5", 1, "1", "ODT", 1, current_time_string, "", "Y", ""),
            ("d", "e", "f", "worf", "id6", 2, "1", "ODT", 1, current_time_string, "", "Y", ""),
            ("e", "f", "g", "kira", "id7", 3, "1", "ODT", 1, current_time_string, "", "Y", ""),
            ("p", "q", "r", "odo", "id8", 4, "1", "ODT", 1, current_time_string, "", "Y", ""),
        ),
        generate_harmonised_table_schema(altered_schema, incremental_key),
    )
    # Run the full etl process
    with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
        with mock.patch.object(Util, "display_dataframe"):
            inst = ServiceBusHarmonisationProcess(spark)
            result = inst.run(entity_name="test_sb_hrm_pc_chg_schema")
            assert_etl_result_successful(result)
            actual_table_data = spark.table("odw_harmonised_db.test_sb_hrm_pc_chg_schema")
            compare_harmonised_data(expected_harmonised_data_after_writing, actual_table_data)


@pytest.mark.parametrize(
    "teardown",
    [
        [
            os.path.join("odw-standardised", "test_sb_hrm_pc_no_data"),
            os.path.join("odw_harmonised_db.db", "test_sb_hrm_pc_no_data"),
            os.path.join("odw-harmonised", "test_sb_hrm_pc_no_data"),
        ]
    ],
    indirect=["teardown"],
)
def test__service_bus_harmonisation_process__run__with_no_existing_data(teardown):
    """
    - Given I the harmonnised table does not exist, and I have some new standardised data to add
    - When I call ServiceBusHarmonisationProcess.run
    - Then the harmonised data should be created
    """
    add_orchestration_entries(
        {
            "Source_Filename_Start": "test_sb_hrm_pc_no_data",
            "Load_Enable_status": "True",
            "Standardised_Table_Definition": "standardised_table_definitions/test_sb_hrm_pc_no_data/test_sb_hrm_pc_no_data.json",
            "Source_Frequency_Folder": "",
            "Standardised_Table_Name": "test_sb_hrm_pc_no_data",
            "Expected_Within_Weekdays": 1,
            "Harmonised_Table_Name": "test_sb_hrm_pc_no_data",
            "Harmonised_Incremental_Key": "incremental_key",
            "Entity_Primary_Key": "col_a",
        },
    )
    spark = SparkSession.builder.getOrCreate()
    table_name = "test_sb_hrm_pc_no_data"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    incremental_key = "incremental_key"
    base_schema = T.StructType(
        [T.StructField("col_a", T.StringType()), T.StructField("col_b", T.StringType()), T.StructField("col_c", T.StringType())]
    )
    existing_data_ingestion_date_string = "2025-09-12T10:30:59.405000+0000"
    # Create existing harmonised table
    existing_harmonised_data = spark.createDataFrame((), generate_harmonised_table_schema(base_schema, incremental_key))
    write_existing_table(existing_harmonised_data, table_name, "odw_harmonised_db", "odw-harmonised", table_name)

    # Create standardised table
    existing_data_ingestion_date = datetime.strptime(existing_data_ingestion_date_string, datetime_format)
    input_file = "some_file"
    already_existing_standardised_col_data = (
        existing_data_ingestion_date,
        existing_data_ingestion_date,
        existing_data_ingestion_date,
        "Create",
        existing_data_ingestion_date_string,
        input_file,
    )
    standardised_data = spark.createDataFrame(
        (
            ("a", "b", "c", "id1") + already_existing_standardised_col_data,
            ("d", "e", "f", "id2") + already_existing_standardised_col_data,
            ("e", "f", "g", "id3") + already_existing_standardised_col_data,
        ),
        generate_standardised_table_schema(base_schema),
    )
    write_existing_table(standardised_data, table_name, "odw_standardised_db", "odw-standardised", table_name)

    expected_harmonised_data_after_writing = spark.createDataFrame(
        (
            ("a", "b", "c", "id1", 1, "1", "ODT", 1, existing_data_ingestion_date_string, "", "Y", ""),
            ("d", "e", "f", "id2", 2, "1", "ODT", 1, existing_data_ingestion_date_string, "", "Y", ""),
            ("e", "f", "g", "id3", 3, "1", "ODT", 1, existing_data_ingestion_date_string, "", "Y", ""),
        ),
        generate_harmonised_table_schema(base_schema, incremental_key),
    )
    # Run the full etl process
    with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
        with mock.patch.object(Util, "display_dataframe"):
            inst = ServiceBusHarmonisationProcess(spark)
            result = inst.run(entity_name="test_sb_hrm_pc_no_data")
            assert_etl_result_successful(result)
            actual_table_data = spark.table("odw_harmonised_db.test_sb_hrm_pc_no_data")
            compare_harmonised_data(expected_harmonised_data_after_writing, actual_table_data)
