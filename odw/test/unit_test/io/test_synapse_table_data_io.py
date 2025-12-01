from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.test.util.assertion import assert_dataframes_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pytest
import mock


def test__synapse_table_data_io__read__successful(spark_sesson: SparkSession, tmpdir):
    test_name = test__synapse_table_data_io__read__successful.__name__
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [
            (1, "a"),
            (2, "b"),
            (3, "c")
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ]
        )
    )
    mock_file_path = f"{tmpdir}/{test_name}"
    mock_table_path = f"{test_name}.sometable"
    spark_sesson.sql(f"CREATE DATABASE IF NOT EXISTS {test_name}")
    mock_dataframe.write.format("parquet").mode("overwrite").option("path", mock_file_path).saveAsTable(mock_table_path)
    database_name = test_name
    table_name = "sometable"
    file_format = "parquet"
    actual_dataframe = SynapseTableDataIO().read(
        database_name=database_name,
        table_name=table_name,
        file_format=file_format,
        spark=spark_sesson
    )
    assert_dataframes_equal(mock_dataframe, actual_dataframe)


@pytest.mark.parametrize(
    "argument_to_drop",
    [
        "database_name",
        "table_name",
        "file_format",
        "spark"
    ]
)
def test__synapse_table_data_io__read__with_missing_arguments(argument_to_drop: str):
    test_name = test__synapse_table_data_io__read__with_missing_arguments.__name__
    all_arguments = {
        "database_name": test_name,
        "table_name": "sometable",
        "storage_name": "somestorageaccount",
        "container_name": "somecontainer",
        "blob_path": "some/path",
        "file_format": "someformat",
        "spark": ""
    }
    all_arguments_cleaned = {k: v for k, v in all_arguments.items() if k != argument_to_drop}
    with mock.patch.object(SynapseTableDataIO, "__init__", return_value=None):
        data_io_inst = SynapseTableDataIO()
        with pytest.raises(ValueError):
            data_io_inst.read(**all_arguments_cleaned)


def test__synapse_table_data_io__write__successful(spark_sesson, tmpdir):
    test_name = test__synapse_table_data_io__write__successful.__name__
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [
            (1, "a"),
            (2, "b"),
            (3, "c")
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ]
        )
    )
    mock_table_path = f"{test_name}.sometable"
    mock_file_path = f"{tmpdir}/{test_name}"
    database_name = test_name
    table_name = "sometable"
    storage_name = "somestorageaccount"
    container_name = "somecontainername"
    blob_path = "some/path/to/a/blob.parquet"
    file_format = "parquet"
    spark_sesson.sql(f"CREATE DATABASE IF NOT EXISTS {test_name}")
    with mock.patch.object(SynapseTableDataIO, "_format_to_adls_path", return_value=mock_file_path):
        SynapseTableDataIO().write(
            data=mock_dataframe,
            database_name=database_name,
            table_name=table_name,
            storage_name=storage_name,
            container_name=container_name,
            blob_path=blob_path,
            file_format=file_format,
            write_mode="overwrite",
            spark=spark_sesson
        )
        written_dataframe = spark_sesson.read.format(file_format).table(mock_table_path)
        assert_dataframes_equal(mock_dataframe, written_dataframe)


@pytest.mark.parametrize(
    "argument_to_drop",
    [
        "database_name",
        "table_name",
        "storage_name",
        "container_name",
        "blob_path",
        "file_format",
        "write_mode"
    ]
)
def test__synapse_table_data_io__write__with_missing_arguments(spark_sesson: SparkSession, argument_to_drop: str):
    test_name = test__synapse_table_data_io__write__successful.__name__
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [],
        StructType([StructField("id", IntegerType(), True),])
    )
    all_arguments = {
        "database_name": test_name,
        "table_name": "sometable",
        "storage_name": "somestorageaccount",
        "container_name": "somecontainer",
        "blob_path": "some/path",
        "file_format": "someformat",
        "write_mode": "somewritemode"
    }
    all_arguments_cleaned = {k: v for k, v in all_arguments.items() if k != argument_to_drop}
    with mock.patch.object(SynapseTableDataIO, "__init__", return_value=None):
        data_io_inst = SynapseTableDataIO()
        with pytest.raises(ValueError):
            data_io_inst.write(mock_dataframe, **all_arguments_cleaned)
