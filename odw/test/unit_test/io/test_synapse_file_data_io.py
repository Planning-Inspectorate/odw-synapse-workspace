from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pytest
import mock


def test__synapse_file_data_io__read__successful(tmpdir):
    spark_sesson = PytestSparkSessionUtil().get_spark_session()
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")], StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
    )
    mock_file_path = f"{tmpdir}/{test__synapse_file_data_io__read__successful}"
    mock_dataframe.write.format("parquet").mode("overwrite").save(mock_file_path)
    storage_name = "somestorageaccount"
    container_name = "somecontainername"
    blob_path = "some/path/to/a/blob.parquet"
    file_format = "parquet"
    with mock.patch.object(SynapseFileDataIO, "_format_to_adls_path", return_value=mock_file_path):
        actual_dataframe = SynapseFileDataIO().read(
            storage_name=storage_name, container_name=container_name, blob_path=blob_path, file_format=file_format, spark=spark_sesson
        )
        assert_dataframes_equal(mock_dataframe, actual_dataframe)


@pytest.mark.parametrize("argument_to_drop", ["storage_name", "container_name", "blob_path", "file_format", "spark"])
def test__synapse_file_data_io__read__with_missing_arguments(argument_to_drop: str):
    all_arguments = {
        "storage_name": "somestorageaccount",
        "container_name": "somecontainer",
        "blob_path": "some/path",
        "file_format": "someformat",
        "spark": "",
    }
    all_arguments_cleaned = {k: v for k, v in all_arguments.items() if k != argument_to_drop}
    with mock.patch.object(SynapseFileDataIO, "__init__", return_value=None):
        data_io_inst = SynapseFileDataIO()
        with pytest.raises(ValueError):
            data_io_inst.read(**all_arguments_cleaned)


def test__synapse_file_data_io__write__successful(tmpdir):
    spark_sesson = PytestSparkSessionUtil().get_spark_session()
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")], StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
    )
    mock_file_path = f"{tmpdir}/{test__synapse_file_data_io__write__successful}"
    storage_name = "somestorageaccount"
    container_name = "somecontainername"
    blob_path = "some/path/to/a/blob.parquet"
    file_format = "parquet"
    with mock.patch.object(SynapseFileDataIO, "_format_to_adls_path", return_value=mock_file_path):
        SynapseFileDataIO().write(
            data=mock_dataframe,
            storage_name=storage_name,
            container_name=container_name,
            blob_path=blob_path,
            file_format=file_format,
            write_mode="overwrite",
            spark=spark_sesson,
        )
        written_dataframe = spark_sesson.read.parquet(mock_file_path)
        assert_dataframes_equal(mock_dataframe, written_dataframe)


@pytest.mark.parametrize("argument_to_drop", ["storage_name", "container_name", "blob_path", "file_format", "write_mode"])
def test__synapse_file_data_io__write__with_missing_arguments(argument_to_drop: str):
    spark_sesson = PytestSparkSessionUtil().get_spark_session()
    mock_dataframe: DataFrame = spark_sesson.createDataFrame(
        [],
        StructType(
            [
                StructField("id", IntegerType(), True),
            ]
        ),
    )
    all_arguments = {
        "storage_name": "somestorageaccount",
        "container_name": "somecontainer",
        "blob_path": "some/path",
        "file_format": "someformat",
        "write_mode": "somewritemode",
    }
    all_arguments_cleaned = {k: v for k, v in all_arguments.items() if k != argument_to_drop}
    with mock.patch.object(SynapseFileDataIO, "__init__", return_value=None):
        data_io_inst = SynapseFileDataIO()
        with pytest.raises(ValueError):
            data_io_inst.write(mock_dataframe, **all_arguments_cleaned)
