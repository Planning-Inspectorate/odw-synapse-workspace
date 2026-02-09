from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil
from datetime import datetime
import mock
from odw.test.util.assertion import assert_dataframes_equal


def test__service_bus_standardisation_process__get_max_file_date():
    spark = PytestSparkSessionUtil().get_spark_session()
    df = spark.createDataFrame(
        [
            (1, "2025-01-01T00:00:00.000000+0000"),
            (2, "1999-01-02T12:00:00.000000+0000"),
            (3, "1984-01-01T00:00:00.000000+0100"),
            (4, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet"),
        ],
        ["col A", "input_file"],
    )
    expected_value = datetime(2030, 12, 1, 0, 0, 0, 0, None)
    with mock.patch.object(LoggingUtil, "__new__"):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                assert expected_value == ServiceBusStandardisationProcess(spark).get_max_file_date(df)


def test__service_bus_standardisation_process__get_missing_files():
    spark = PytestSparkSessionUtil().get_spark_session()
    df = spark.createDataFrame(
        [
            (1, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile1.parquet"),
            (2, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2.parquet"),
            (3, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile4.parquet"),
            (4, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/other-entity/myfile1.parquet"),
        ],
        ["col A", "input_file"],
    )
    source_path = "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/"
    mock_get_all_files_in_directory = [
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile1.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile3.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile4.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile5.parquet",
    ]
    expected_output = {
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile3.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile5.parquet",
    }
    with mock.patch.object(LoggingUtil, "__new__"):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                with mock.patch.object(ServiceBusStandardisationProcess, "get_all_files_in_directory", return_value=mock_get_all_files_in_directory):
                    actual_output = ServiceBusStandardisationProcess(spark).get_missing_files(df, source_path)
                    assert expected_output == set(actual_output)


def test__service_bus_standardisation_process__extract_and_filter_paths():
    spark = PytestSparkSessionUtil().get_spark_session()
    files = [
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2025-01-01T00:00:00.000000+0000.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile1999-01-02T12:00:00.000000+0000.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile1984-01-01T00:00:00.000000+0100.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet",
    ]
    date = datetime.strptime("2024-12-01T00:00:00.000000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
    expected_output = {
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2025-01-01T00:00:00.000000+0000.parquet",
        "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet",
    }
    with mock.patch.object(LoggingUtil, "__new__"):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                actual_output = ServiceBusStandardisationProcess(spark).extract_and_filter_paths(files, date)
                assert expected_output == set(actual_output)


def test__service_bus_standardisation_process__read_raw_messages():
    pass


def test__service_bus_standardisation_process__remove_data_duplicates():
    spark = PytestSparkSessionUtil().get_spark_session()
    df = spark.createDataFrame(
        [
            (1, "2025-01-01T00:00:00.000000+0000", 1, 2, 3),
            (1, "2025-01-01T00:00:00.000000+0000", 2, 4, 6),  # A duplicate of the first row
            (3, "1984-01-01T00:00:00.000000+0100", 4, 5, 6),
            (3, "1984-01-01T00:00:00.000000+0100", 4, 5, 6),  # A duplicate of the 2nd row
            (4, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet", 2, 4, 5),
            (5, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet", 1, 4, 6),
        ],
        ["col A", "input_file", "expected_to", "expected_from", "ingested_datetime"],
    )
    expected_output = spark.createDataFrame(
        [
            (1, "2025-01-01T00:00:00.000000+0000", 1, 2, 3),
            (3, "1984-01-01T00:00:00.000000+0100", 4, 5, 6),
            (4, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet", 2, 4, 5),
            (5, "abfss://odw-raw@mystorageaccount.dfs.core.windows.net/ServiceBus/appeal-has/myfile2030-12-01T00:00:00.000000+0000.parquet", 1, 4, 6),
        ],
        ["col A", "input_file", "expected_to", "expected_from", "ingested_datetime"],
    )
    with mock.patch.object(LoggingUtil, "__new__"):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                actual_output = ServiceBusStandardisationProcess(spark).remove_data_duplicates(df)
                expected_output.show()
                actual_output.show()
                assert_dataframes_equal(expected_output, actual_output)


def test__service_bus_standardisation_process__process():
    spark = PytestSparkSessionUtil().get_spark_session()

    df = spark.createDataFrame(
        [(1, "a", 11, 11, [1, 2]), (2, "b", 22, 22, [3, 4]), (3, "c", 33, 33, [5, 6]), (4, "d", 44, 44, [7, 8]), (5, "e", 55, 55, [9])],
        ["col A", "col B", "col C", "col_c", "col D"],
    )
