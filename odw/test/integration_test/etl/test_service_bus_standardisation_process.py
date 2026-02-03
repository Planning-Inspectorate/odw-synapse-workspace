from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.etl.util.schema_util import SchemaUtil
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path
from odw.test.util.util import get_all_files_in_directory, format_to_adls_path
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import mock
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import json
import pytest
import shutil
import os
from pathlib import Path


@pytest.fixture
def teardown(request: pytest.FixtureRequest):
    yield
    shutil.rmtree(
        generate_local_path(request.param),
        ignore_errors=True
    )


@pytest.mark.parametrize(
    "teardown",
    [
        os.path.join("odw-standardised", "test__service_bus_standardisation_process__run")
    ],
    indirect=["teardown"]
)
def test__service_bus_standardisation_process__run__with_existing_data(teardown):
    """
    - Given I have data stored in the sb_test__service_bus_standardisation_process__run table (in delta format)
      - The table has two entries that refer to files myfile1 and myfile2
      - myfile1 and myfile2 contain new rows to be added to the table
    - When I run ServiceBusStandardisationProcess
    - Then the sb_test__service_bus_standardisation_process__run table should be updated with new content
    """
    spark = SparkSession.builder.getOrCreate()
    entity_name = "test__service_bus_standardisation_process__run"
    message_enqueued_time_utc = "2024-05-22T14:24:14.261000+0000"
    test_data = spark.createDataFrame(
        [
            (1, "a", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile1.json", message_enqueued_time_utc),
            (2, "b", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile1.json", message_enqueued_time_utc),
            (3, "c", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile2.json", message_enqueued_time_utc),
        ],
        ["col_a", "col_b", "input_file", "message_enqueued_time_utc"],
    )
    input_files = {
        f"odw-raw/ServiceBus/{entity_name}/myfile1.json": [
            {"col_a": 1, "col_b": "a", "message_enqueued_time_utc": message_enqueued_time_utc},
            {"col_a": 2, "col_b": "b", "message_enqueued_time_utc": message_enqueued_time_utc},
        ],
        f"odw-raw/ServiceBus/{entity_name}/myfile2.json": [
            {"col_a": 3, "col_b": "c", "message_enqueued_time_utc": message_enqueued_time_utc},
            {"col_a": 4, "col_b": "d", "message_enqueued_time_utc": message_enqueued_time_utc},  # A new row
            {"col_a": 5, "col_b": "e", "message_enqueued_time_utc": message_enqueued_time_utc},  # A new row
        ]
    }
    for input_file, content in input_files.items():
        file_to_create = f"spark-warehouse/{input_file}"
        file_path = file_to_create.rsplit("/", 1)[0]
        Path(file_path).mkdir(parents=True, exist_ok=True)
        with open(file_to_create, "w") as f:
            json.dump(content, f)

    expected_data_after = spark.createDataFrame(
        [
            (1, "a", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile1.json", message_enqueued_time_utc),
            (2, "b", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile1.json", message_enqueued_time_utc),
            (3, "c", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile2.json", message_enqueued_time_utc),
            (4, "d", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile2.json", message_enqueued_time_utc),
            (5, "e", f"file://{Path().resolve()}/odw-raw/ServiceBus/{entity_name}/myfile2.json", message_enqueued_time_utc),
        ],
        ["col_a", "col_b", "input_file", "message_enqueued_time_utc"],
    )
    # Duplicate data is expected (although this probably shouldn't be the case)
    expected_data_after = expected_data_after.union(test_data)
    extra_cols_added_during_processing = ["expected_from", "expected_to", "ingested_datetime", "input_file"]
    mock_standardised_schema = T.StructType(
        [
            T.StructField("col_a", T.LongType()),
            T.StructField("col_b", T.StringType()),
            T.StructField("input_file", T.StringType()),
            T.StructField("message_enqueued_time_utc", T.StringType())
        ]
    )

    with mock.patch.object(SynapseTableDataIO, "_format_to_adls_path", format_to_adls_path):
        SynapseTableDataIO().write(
            data=test_data,
            spark=spark,
            database_name="odw_standardised_db",
            table_name="sb_test__service_bus_standardisation_process__run",
            storage_name="blank",
            container_name="odw-standardised",
            blob_path="test__service_bus_standardisation_process__run",
            file_format="delta",
            write_mode="append",
            write_options=[("mergeSchema", "true")]
        )
        data_before = spark.table("odw_standardised_db.sb_test__service_bus_standardisation_process__run")
        mock_mssparkutils_context = {"pipelinejobid": "some_guid", "isForPipeline": True}
        with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
            with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
                with mock.patch.object(ServiceBusStandardisationProcess, "get_all_files_in_directory", get_all_files_in_directory):
                    with mock.patch.object(Util, "get_path_to_file", generate_local_path):
                        with mock.patch.object(LoggingUtil, "__new__"):
                            with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                                with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                                    with mock.patch.object(SchemaUtil, "get_service_bus_schema", return_value=mock_standardised_schema):
                                        result = ServiceBusStandardisationProcess(spark).run(entity_name="test__service_bus_standardisation_process__run")
                                        assert_etl_result_successful(result)
    data_after = spark.table("odw_standardised_db.sb_test__service_bus_standardisation_process__run")
    # Drop columns that cannot easily be compared - todo actually compare these cols
    for col in extra_cols_added_during_processing:
        data_after = data_after.drop(col)
        if col in expected_data_after.columns:
            expected_data_after = expected_data_after.drop(col)
    assert_dataframes_equal(expected_data_after, data_after)
