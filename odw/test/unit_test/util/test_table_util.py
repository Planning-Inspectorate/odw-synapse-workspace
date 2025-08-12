from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.table_util import TableUtil
from pyspark.sql import SparkSession
from pyspark.sql import Catalog
import pytest
import mock
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, TimestampType, ArrayType, MapType
from typing import List, Dict


DESCRIPTION_SCHEMA = StructType(
    [
        StructField("format", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("location", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("lastModified", TimestampType(), True),
        StructField("partitionColumns", ArrayType(StringType()), True),
        StructField("numFiles", IntegerType(), True),
        StructField("sizeInBytes", LongType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
        StructField("minReaderVersion", IntegerType(), True),
        StructField("minWriterVersion", IntegerType(), True),
        StructField("tableFeatures", ArrayType(StringType()), True)
    ]
)


def get_sample_table_dataframe(table_details: List[Dict[str, str]], table_kind: str):
    spark = SparkSession.builder.getOrCreate()
    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    return spark.createDataFrame(
        [
            (
                table_kind,
                "some_guid",
                f"spark_catalog.{details['db_name']}.{details['table_name']}",
                None,
                details['table_location'],
                datetime.strptime("2024-01-01 00:00:00.000000", datetime_format),
                datetime.strptime("2025-01-01 00:00:00.000000", datetime_format),
                [],
                20000,
                4000000000,
                {},
                1,
                2,
                ["appendOnly", "invariants"]
            )
            for details in table_details
        ],
        DESCRIPTION_SCHEMA
    )


def test_delete_table__successful():
    db_name = "some_db"
    table_name = "some_table"
    storage_name = "somestorageaccount"
    table_location = f"abfss://{db_name.replace('_', '-')}@{storage_name}.dfs.core.windows.net/some/table_name"
    with mock.patch.object(Catalog, "tableExists", return_value=True):
        mock_df = get_sample_table_dataframe([{"db_name": db_name, "table_name": table_name, "table_location": table_location}], "parquet")
        with mock.patch.object(SparkSession, "sql", return_value=mock_df):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_exception", return_value=None):
                        notebookutils.mssparkutils.fs.rm = mock.MagicMock()
                        notebookutils.mssparkutils.fs.rm.return_value = None
                        TableUtil.delete_table(db_name, table_name)
                        SparkSession.sql.assert_has_calls(
                            [
                                mock.call(f"DESCRIBE DETAIL {db_name}.{table_name}"),
                                mock.call(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
                            ]
                        )
                        notebookutils.mssparkutils.fs.rm.assert_called_once_with(table_location, True)


def test_delete_table__table_does_not_exist():
    db_name = "some_db"
    table_name = "some_table"
    with mock.patch.object(Catalog, "tableExists", return_value=False):
        with mock.patch.object(SparkSession, "sql", return_value=None):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    notebookutils.mssparkutils.fs.rm = mock.MagicMock()
                    notebookutils.mssparkutils.fs.rm.return_value = None
                    TableUtil.delete_table(db_name, table_name)
                    assert not SparkSession.sql.called
                    LoggingUtil.log_info.assert_has_calls(
                        [
                            mock.call("Table does not exist")
                        ],
                        any_order=True
                    )
                    assert not notebookutils.mssparkutils.fs.rm.called


def test_delete_table__multiple_occurrences():
    db_name = "some_db"
    table_name = "some_table"
    storage_name = "somestorageaccount"
    table_location = f"abfss://{db_name.replace('_', '-')}@{storage_name}.dfs.core.windows.net/some/table_name"
    with mock.patch.object(Catalog, "tableExists", return_value=True):
        mock_df = get_sample_table_dataframe(
            [
                {"db_name": db_name, "table_name": table_name, "table_location": table_location},
                {"db_name": db_name, "table_name": table_name, "table_location": "some_other_location"}
            ],
            "parquet"
        )
        with mock.patch.object(SparkSession, "sql", return_value=mock_df):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_exception", return_value=None):
                        with pytest.raises(RuntimeError):
                            notebookutils.mssparkutils.fs.rm = mock.MagicMock()
                            notebookutils.mssparkutils.fs.rm.return_value = None
                            TableUtil.delete_table(db_name, table_name)
                        SparkSession.sql.assert_called_once_with(f"DESCRIBE DETAIL {db_name}.{table_name}")
                        assert not notebookutils.mssparkutils.fs.rm.called


def test_delete_table_contents__successful():
    db_name = "some_db"
    table_name = "some_table"
    storage_name = "somestorageaccount"
    table_location = f"abfss://{db_name.replace('_', '-')}@{storage_name}.dfs.core.windows.net/some/table_name"
    with mock.patch.object(Catalog, "tableExists", return_value=True):
        mock_df = get_sample_table_dataframe([{"db_name": db_name, "table_name": table_name, "table_location": table_location}], "delta")
        with mock.patch.object(SparkSession, "sql", return_value=mock_df):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_exception", return_value=None):
                        notebookutils.mssparkutils.fs.rm = mock.MagicMock()
                        notebookutils.mssparkutils.fs.rm.return_value = None
                        TableUtil.delete_table_contents(db_name, table_name)
                        SparkSession.sql.assert_has_calls(
                            [
                                mock.call(f"DESCRIBE DETAIL {db_name}.{table_name}"),
                                mock.call(f"DELETE FROM {db_name}.{table_name}")
                            ]
                        )
                        # Should not delete the underlying file system
                        assert not notebookutils.mssparkutils.fs.rm.called


def test_delete_table_contents__table_does_not_exist():
    db_name = "some_db"
    table_name = "some_table"
    with mock.patch.object(Catalog, "tableExists", return_value=False):
        with mock.patch.object(SparkSession, "sql", return_value=None):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    TableUtil.delete_table_contents(db_name, table_name)
                    assert not SparkSession.sql.called
                    LoggingUtil.log_info.assert_has_calls(
                        [
                            mock.call("Table does not exist")
                        ],
                        any_order=True
                    )


def test_delete_table_contents__multiple_occurrences():
    db_name = "some_db"
    table_name = "some_table"
    storage_name = "somestorageaccount"
    table_location = f"abfss://{db_name.replace('_', '-')}@{storage_name}.dfs.core.windows.net/some/table_name"
    with mock.patch.object(Catalog, "tableExists", return_value=True):
        mock_df = get_sample_table_dataframe(
            [
                {"db_name": db_name, "table_name": table_name, "table_location": table_location},
                {"db_name": db_name, "table_name": table_name, "table_location": "some_other_location"}
            ],
            "delta"
        )
        with mock.patch.object(SparkSession, "sql", return_value=mock_df):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_exception", return_value=None):
                        with pytest.raises(RuntimeError):
                            notebookutils.mssparkutils.fs.rm = mock.MagicMock()
                            notebookutils.mssparkutils.fs.rm.return_value = None
                            TableUtil.delete_table_contents(db_name, table_name)
                        SparkSession.sql.assert_called_once_with(f"DESCRIBE DETAIL {db_name}.{table_name}")
                        assert not notebookutils.mssparkutils.fs.rm.called
