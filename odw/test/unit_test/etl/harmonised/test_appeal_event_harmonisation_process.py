from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from odw.core.etl.transformation.harmonised.appeal_event_harmonisation_process import AppealEventHarmonisationProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


pytestmark = pytest.mark.xfail(
    raises=NotImplementedError,
    reason="Harmonised logic not implemented yet",
)

HARMONISED_COLUMNS = [
    "AppealsEventId",
    "eventId",
    "caseReference",
    "eventType",
    "eventName",
    "eventStatus",
    "isUrgent",
    "eventPublished",
    "eventStartDatetime",
    "eventEndDatetime",
    "notificationOfSitevisit",
    "addressLine1",
    "addressLine2",
    "addressTown",
    "addressCounty",
    "addressPostcode",
    "Migrated",
    "ODTSourceSystem",
    "SourceSystemID",
    "IngestionDate",
    "ValidTo",
    "RowID",
    "IsActive",
]


def _service_bus_schema():
    return T.StructType(
        [
            T.StructField("AppealsEventId", T.LongType(), True),
            T.StructField("eventId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("eventType", T.StringType(), True),
            T.StructField("eventName", T.StringType(), True),
            T.StructField("eventstatus", T.StringType(), True),
            T.StructField("isUrgent", T.BooleanType(), True),
            T.StructField("eventPublished", T.BooleanType(), True),
            T.StructField("eventStartDatetime", T.StringType(), True),
            T.StructField("eventEndDatetime", T.StringType(), True),
            T.StructField("notificationOfSitevisit", T.StringType(), True),
            T.StructField("AddressLine1", T.StringType(), True),
            T.StructField("AddressLine2", T.StringType(), True),
            T.StructField("AddressTown", T.StringType(), True),
            T.StructField("AddressCounty", T.StringType(), True),
            T.StructField("AddressPostcode", T.StringType(), True),
            T.StructField("Migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("SourceSystemID", T.IntegerType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _horizon_schema():
    return T.StructType(
        [
            T.StructField("casenumber", T.StringType(), True),
            T.StructField("eventId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("eventType", T.StringType(), True),
            T.StructField("eventName", T.StringType(), True),
            T.StructField("eventStatus", T.StringType(), True),
            T.StructField("notificationOfSitevisit", T.StringType(), True),
            T.StructField("eventaddressline1", T.StringType(), True),
            T.StructField("eventaddressline2", T.StringType(), True),
            T.StructField("eventaddresstown", T.StringType(), True),
            T.StructField("eventaddresscounty", T.StringType(), True),
            T.StructField("eventaddresspostcode", T.StringType(), True),
            T.StructField("ingested_datetime", T.TimestampType(), True),
        ]
    )


def _empty_service_bus_df(spark):
    return spark.createDataFrame([], _service_bus_schema())


def _empty_horizon_df(spark):
    return spark.createDataFrame([], _horizon_schema())


def _service_bus_df(spark):
    return spark.createDataFrame(
        [
            (
                999,
                "EVT-001",
                "APP-001",
                "hearing",
                "Old hearing",
                "scheduled",
                True,
                True,
                "2025-01-01T09:00:00",
                "2025-01-01T10:00:00",
                "N",
                "Old line 1",
                "Old line 2",
                "Old town",
                "Old county",
                "AA1 1AA",
                "legacy-value",
                "ODT",
                1,
                datetime(2025, 1, 1, 9, 0, 0),
                None,
                "old-row-id",
                "Y",
            ),
            (
                1000,
                "EVT-001",
                "APP-001",
                "hearing",
                "New hearing",
                "complete",
                False,
                True,
                "2025-01-02T09:00:00",
                "2025-01-02T10:00:00",
                "Y",
                "New line 1",
                "New line 2",
                "New town",
                "New county",
                "AA1 1AA",
                "legacy-value",
                "ODT",
                1,
                datetime(2025, 1, 2, 9, 0, 0),
                None,
                "new-row-id",
                "Y",
            ),
        ],
        _service_bus_schema(),
    )


def _horizon_df(spark):
    return spark.createDataFrame(
        [
            (
                "CASE-HZN",
                "1",
                "APP-HZN",
                "site_visit",
                "Old Horizon event",
                "old",
                "N",
                "Old H line 1",
                "Old H line 2",
                "Old H town",
                "Old H county",
                "HH1 1HH",
                datetime(2025, 1, 1, 8, 0, 0),
            ),
            (
                "CASE-HZN",
                "1",
                "APP-HZN",
                "site_visit",
                "Latest Horizon event",
                "scheduled",
                "Y",
                "H line 1",
                "H line 2",
                "H town",
                "H county",
                "HH1 1HH",
                datetime(2025, 1, 3, 8, 0, 0),
            ),
        ],
        _horizon_schema(),
    )


def _duplicate_service_bus_df(spark):
    service_bus = _service_bus_df(spark)
    return service_bus.unionByName(service_bus)


def _source_data(service_bus_data, horizon_data):
    return {
        "service_bus_data": service_bus_data,
        "horizon_data": horizon_data,
    }


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.harmonised.harmonsation_process.HarmonisationProcess.__init__",
        return_value=None,
    ):
        inst = AppealEventHarmonisationProcess(spark)

    inst.spark = spark
    return inst


class TestAppealEventHarmonisationProcess(SparkTestCase):
    def test__appeal_event_harmonisation_process__process__merges_service_bus_and_latest_horizon_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        data_to_write, result = inst.process(_source_data(_service_bus_df(spark), _horizon_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.columns == HARMONISED_COLUMNS
        assert write_config["write_mode"].lower() == "overwrite"
        assert write_config["file_format"] == "delta"
        assert write_config["partition_by"] == ["IsActive"]

        actual_df = df.select(
            "AppealsEventId",
            "eventId",
            "caseReference",
            "eventName",
            "eventStatus",
            "isUrgent",
            "eventPublished",
            "addressLine1",
            "addressTown",
            "ODTSourceSystem",
            "SourceSystemID",
            "Migrated",
            "IngestionDate",
            "ValidTo",
            "IsActive",
        ).orderBy("eventId", "IngestionDate")

        expected_df = spark.createDataFrame(
            [
                (
                    3,
                    "CASE-HZN-1",
                    "APP-HZN",
                    "Latest Horizon event",
                    "scheduled",
                    None,
                    None,
                    "H line 1",
                    "H town",
                    "Horizon",
                    9,
                    "0",
                    datetime(2025, 1, 3, 8, 0, 0),
                    None,
                    "Y",
                ),
                (
                    1,
                    "EVT-001",
                    "APP-001",
                    "Old hearing",
                    "scheduled",
                    True,
                    True,
                    "Old line 1",
                    "Old town",
                    "ODT",
                    1,
                    "1",
                    datetime(2025, 1, 1, 9, 0, 0),
                    datetime(2025, 1, 2, 9, 0, 0),
                    "N",
                ),
                (
                    2,
                    "EVT-001",
                    "APP-001",
                    "New hearing",
                    "complete",
                    False,
                    True,
                    "New line 1",
                    "New town",
                    "ODT",
                    1,
                    "1",
                    datetime(2025, 1, 2, 9, 0, 0),
                    None,
                    "Y",
                ),
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

        row_ids = [row["RowID"] for row in df.select("RowID").collect()]

        assert all(row_id is not None for row_id in row_ids)
        assert all(len(row_id) == 32 for row_id in row_ids)
        assert "old-row-id" not in row_ids
        assert "new-row-id" not in row_ids

        assert result.metadata.insert_count == 3
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_event_harmonisation_process__process__uses_latest_horizon_ingestion_only_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        data_to_write, result = inst.process(_source_data(_empty_service_bus_df(spark), _horizon_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["eventName"] == "Latest Horizon event"
        assert result.metadata.insert_count == 1

    def test__appeal_event_harmonisation_process__process__applies_distinct_and_drop_duplicates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        data_to_write, result = inst.process(_source_data(_duplicate_service_bus_df(spark), _empty_horizon_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_event_harmonisation_process__process__empty_sources_write_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        data_to_write, result = inst.process(_source_data(_empty_service_bus_df(spark), _empty_horizon_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == HARMONISED_COLUMNS
        assert result.metadata.insert_count == 0

    def test__appeal_event_harmonisation_process__process__generates_row_id_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        data_to_write, result = inst.process(_source_data(_service_bus_df(spark), _empty_horizon_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        row_ids = [row["RowID"] for row in df.select("RowID").collect()]

        assert all(row_id is not None for row_id in row_ids)
        assert all(len(row_id) == 32 for row_id in row_ids)
        assert "old-row-id" not in row_ids
        assert "new-row-id" not in row_ids

        assert result.metadata.insert_count == 2
