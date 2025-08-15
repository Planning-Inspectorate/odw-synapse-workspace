from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.table_util import TableUtil
from odw.test.util.config import TEST_CONFIG
from pyspark.sql import SparkSession
import pytest
import mock
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, TimestampType, ArrayType, MapType
from pyspark.errors.exceptions.captured import AnalysisException
from typing import Callable


SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_SERVER_VERSION = 18
spark = SparkSession.builder.getOrCreate()


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


def validate_table_deleted(database_name: str, table_name: str, raw_data_path: str, function_to_test: Callable):
    with mock.patch.object(notebookutils.mssparkutils.fs, "rm", return_value=None):
        mock_mssparkutils_context = {
            "pipelinejobid": "some_guid",
            "isForPipeline": True
        }
        app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
        with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
            with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value=app_insights_connection_string):
                # Mock flush_logging to save time, since this 1 minute each time it is called
                with mock.patch.object(LoggingUtil, "flush_logging", return_value=None):
                    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
                    mock_table_details = spark.createDataFrame(
                        [
                            (
                                "parquet",
                                "some_guid",
                                f"spark_catalog.{database_name}.{table_name}",
                                None,
                                raw_data_path,
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
                        ],
                        DESCRIPTION_SCHEMA
                    )
                    real_sql_function = spark.sql
                    def sql_side_effect(sql_query: str):
                        # A wrapper around spark.sql, to allow the DESCRIBE DETAIL call to be overridden
                        if "DESCRIBE DETAIL" in sql_query:
                            return mock_table_details
                        return real_sql_function(sql_query) 

                    with mock.patch.object(SparkSession, "sql", side_effect=sql_side_effect):
                        function_to_test(database_name, table_name)
                    # Table should not exist after delete_table is called
                    with pytest.raises(AnalysisException):
                        spark.sql(f"select * from {database_name}.{table_name}")


def create_table(data_format: str, file_path: str, database_name: str, table_name: str):
    # Create the database table
    data = spark.createDataFrame(
        [
            ("Jean-Luc Picard", "Captain"),
            ("William Riker", "Commander"),
            ("Data", "Lt, Commander"),
        ],
        schema=["Name", "Rank"]
    )
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    data.write.format(data_format).mode("overwrite").save(file_path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING {data_format}
        LOCATION '{file_path}'
    """)


def test_delete_parquet_table():
    parquet_file_path = "/tmp/out/test_table_util__test_delete_parquet_table.parquet"
    database_name = "test_table_util"
    table_name = "test_delete_parquet_table"
    create_table("parquet", parquet_file_path, database_name, table_name)
    validate_table_deleted(database_name, table_name, parquet_file_path, TableUtil.delete_table)


@pytest.mark.skip("Cannot test this due to compatibility issue when running outside of Synapse. Need to investigate this further")
def test_delete_delta_table():
    parquet_file_path = "/tmp/out/test_table_util__test_delete_delta_table.parquet"
    database_name = "test_table_util"
    table_name = "test_delete_delta_table"
    # Note: Writing delta tables requires third-party packages. Logically the process would work on any kind
    # of data source, so for simplicity this is being tested against parquet for now
    create_table("parquet", parquet_file_path, database_name, table_name)
    validate_table_deleted(database_name, table_name, parquet_file_path, TableUtil.delete_table_contents)
