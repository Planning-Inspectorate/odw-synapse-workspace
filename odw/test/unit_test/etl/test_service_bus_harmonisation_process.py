from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
from datetime import datetime
import mock
from odw.test.util.assertion import assert_dataframes_equal


def test__service_bus_harmonisation_process__test_harmonise():
    spark = PytestSparkSessionUtil().get_spark_session()
    source_system_data = spark.createDataFrame(
        (("1", "Casework", "", None, "", "Y"),),
        T.StructType(
            [
                T.StructField("SourceSystemId", T.StringType()),
                T.StructField("Description", T.StringType()),
                T.StructField("IngestionDate", T.StringType()),
                T.StructField("ValidTo", T.StringType()),
                T.StructField("RowID", T.StringType()),
                T.StructField("IsActive", T.StringType()),
            ]
        ),
    )
    existing_data_ingestion_date_string = "2025-09-12T10:30:59.405000+0000"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    enqueued_time = datetime.strptime(existing_data_ingestion_date_string, datetime_format)
    standardised_data = spark.createDataFrame(
        [
            ("a", "b", "c", enqueued_time, "", "", "", ""),
            ("d", "e", "f", enqueued_time, "", "", "", ""),
            ("g", "h", "i", enqueued_time, "", "", "", ""),
            ("j", "k", "l", enqueued_time, "", "", "", ""),
            ("j", "k", "l", enqueued_time, "", "", "", ""),
        ],
        ["colA", "colB", "colC", "message_enqueued_time_utc", "ingested_datetime", "expected_from", "expected_to", "input_file"],
    )
    primary_key = "colA"
    incremental_key = "incremental_key"
    expected_harmonised_data = spark.createDataFrame(
        [
            ("a", "b", "c", "1", "", "1", "ODT", "", "Y", 1, existing_data_ingestion_date_string),
            ("d", "e", "f", "1", "", "1", "ODT", "", "Y", 2, existing_data_ingestion_date_string),
            ("g", "h", "i", "1", "", "1", "ODT", "", "Y", 3, existing_data_ingestion_date_string),
            ("j", "k", "l", "1", "", "1", "ODT", "", "Y", 4, existing_data_ingestion_date_string),
        ],
        T.StructType(
            [
                T.StructField("colA", T.StringType(), True),
                T.StructField("colB", T.StringType(), True),
                T.StructField("colC", T.StringType(), True),
                T.StructField("SourceSystemID", T.StringType(), False),
                T.StructField("RowID", T.StringType(), False),
                T.StructField("migrated", T.StringType(), False),
                T.StructField("ODTSourceSystem", T.StringType(), False),
                T.StructField("ValidTo", T.StringType(), False),
                T.StructField("IsActive", T.StringType(), False),
                T.StructField(incremental_key, T.LongType(), True),
                T.StructField("IngestionDate", T.StringType(), True),
            ]
        ),
    )
    with mock.patch.object(ServiceBusHarmonisationProcess, "__init__", return_value=None):
        inst = ServiceBusHarmonisationProcess()
        actual_harmonised_data = inst.harmonise(standardised_data, source_system_data, incremental_key, primary_key)
        expected_harmonised_data = expected_harmonised_data.drop(incremental_key).drop("IngestionDate")
        actual_harmonised_data = actual_harmonised_data.drop(incremental_key).drop("IngestionDate")
        assert_dataframes_equal(expected_harmonised_data, actual_harmonised_data)
