from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.appeal_event_harmonisation_process import AppealEventHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.skip(
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


def _existing_harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                1,
                "OLD-EVENT",
                "OLD-CASE",
                "old type",
                "old name",
                "old status",
                False,
                False,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "0",
                "ODT",
                1,
                datetime(2020, 1, 1, 0, 0, 0),
                None,
                "old-row-id",
                "Y",
            )
        ],
        T.StructType(
            [
                T.StructField("AppealsEventId", T.LongType(), True),
                T.StructField("eventId", T.StringType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("eventType", T.StringType(), True),
                T.StructField("eventName", T.StringType(), True),
                T.StructField("eventStatus", T.StringType(), True),
                T.StructField("isUrgent", T.BooleanType(), True),
                T.StructField("eventPublished", T.BooleanType(), True),
                T.StructField("eventStartDatetime", T.StringType(), True),
                T.StructField("eventEndDatetime", T.StringType(), True),
                T.StructField("notificationOfSitevisit", T.StringType(), True),
                T.StructField("addressLine1", T.StringType(), True),
                T.StructField("addressLine2", T.StringType(), True),
                T.StructField("addressTown", T.StringType(), True),
                T.StructField("addressCounty", T.StringType(), True),
                T.StructField("addressPostcode", T.StringType(), True),
                T.StructField("Migrated", T.StringType(), True),
                T.StructField("ODTSourceSystem", T.StringType(), True),
                T.StructField("SourceSystemID", T.IntegerType(), True),
                T.StructField("IngestionDate", T.TimestampType(), True),
                T.StructField("ValidTo", T.TimestampType(), True),
                T.StructField("RowID", T.StringType(), True),
                T.StructField("IsActive", T.StringType(), True),
            ]
        ),
    )


class TestAppealEventHarmonisationProcess(ETLTestCase):
    def _run_process(self, spark, test_case: str, service_bus_df, horizon_df):
        service_bus_table = f"{test_case}_sb_appeal_event"
        horizon_table = f"{test_case}_horizon_appeals_event"
        output_table = f"{test_case}_appeal_event"

        self.write_existing_table(
            spark,
            service_bus_df,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

        self.write_existing_table(
            spark,
            horizon_df,
            horizon_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_table,
            "overwrite",
        )

        with (
            mock.patch.object(
                AppealEventHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                AppealEventHarmonisationProcess,
                "HORIZON_TABLE",
                f"odw_standardised_db.{horizon_table}",
            ),
            mock.patch.object(
                AppealEventHarmonisationProcess,
                "OUTPUT_TABLE",
                output_table,
            ),
        ):
            inst = AppealEventHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="appeal_event", orchestration_stage_name="harmonise")
            self._assert_etl_successful_or_raise_not_implemented(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")

        return actual_df, result

    def _assert_harmonisation(self, spark, actual_df, result):
        assert actual_df.columns == HARMONISED_COLUMNS

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 3,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_selected_df = actual_df.select(
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
            actual_selected_df.schema,
        )

        assert_dataframes_equal(actual_selected_df, expected_df)

        row_ids = [row["RowID"] for row in actual_df.select("RowID").collect()]

        assert all(row_id is not None for row_id in row_ids)
        assert all(len(row_id) == 32 for row_id in row_ids)
        assert "old-row-id" not in row_ids
        assert "new-row-id" not in row_ids

    def _assert_etl_successful_or_raise_not_implemented(self, result):
        exception_trace = result.metadata.exception_trace or ""

        if "NotImplementedError" in exception_trace:
            raise NotImplementedError(exception_trace)

        assert_etl_result_successful(result)

    def test__appeal_event_harmonisation_process__run__creates_output_table_when_missing_like_legacy(self):
        test_case = "t_aehp_r_cotwm"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(
            spark,
            test_case,
            _service_bus_df(spark),
            _horizon_df(spark),
        )

        self._assert_harmonisation(spark, actual_df, result)

    def test__appeal_event_harmonisation_process__run__overwrites_existing_output_table_like_legacy(self):
        test_case = "t_aehp_r_oeot"
        spark = PytestSparkSessionUtil().get_spark_session()

        output_table = f"{test_case}_appeal_event"

        self.write_existing_table(
            spark,
            _existing_harmonised_df(spark),
            output_table,
            "odw_harmonised_db",
            "odw-harmonised",
            output_table,
            "overwrite",
        )

        actual_df, result = self._run_process(
            spark,
            test_case,
            _service_bus_df(spark),
            _horizon_df(spark),
        )

        assert actual_df.where("eventId = 'OLD-EVENT'").count() == 0

        self._assert_harmonisation(spark, actual_df, result)
