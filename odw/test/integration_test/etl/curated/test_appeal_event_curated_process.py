import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_event_curated_process import AppealEventCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


CURATED_COLUMNS = [
    "eventId",
    "caseReference",
    "eventType",
    "eventName",
    "eventStatus",
    "isUrgent",
    "eventPublished",
    "eventStartDateTime",
    "eventEndDateTime",
    "notificationOfSiteVisit",
    "addressLine1",
    "addressLine2",
    "addressTown",
    "addressCounty",
    "addressPostcode",
]


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("eventId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("eventType", T.StringType(), True),
            T.StructField("eventName", T.StringType(), True),
            T.StructField("eventStatus", T.StringType(), True),
            T.StructField("IsUrgent", T.BooleanType(), True),
            T.StructField("eventPublished", T.BooleanType(), True),
            T.StructField("eventStartDateTime", T.StringType(), True),
            T.StructField("eventEndDateTime", T.StringType(), True),
            T.StructField("notificationOfSitevisit", T.StringType(), True),
            T.StructField("addressLine1", T.StringType(), True),
            T.StructField("addressLine2", T.StringType(), True),
            T.StructField("addressTown", T.StringType(), True),
            T.StructField("addressCounty", T.StringType(), True),
            T.StructField("addressPostcode", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _harmonised_df(spark):
    df = spark.createDataFrame(
        [
            (
                "EVT-001",
                "APP-001",
                "hearing",
                "Hearing event",
                "scheduled",
                True,
                False,
                "2025-01-01T09:00:00",
                "2025-01-01T10:00:00",
                "Y",
                "Line 1",
                "Line 2",
                "Town",
                "County",
                "AA1 1AA",
                "Y",
            ),
            (
                "EVT-002",
                "APP-002",
                "hearing",
                "Inactive event",
                "complete",
                False,
                True,
                "2025-01-02T09:00:00",
                "2025-01-02T10:00:00",
                "N",
                "Inactive line 1",
                "Inactive line 2",
                "Inactive town",
                "Inactive county",
                "BB1 1BB",
                "N",
            ),
            (
                "EVT-003",
                "APP-003",
                "site_visit",
                "Null active event",
                "cancelled",
                False,
                False,
                "2025-01-03T09:00:00",
                "2025-01-03T10:00:00",
                None,
                "Null line 1",
                "Null line 2",
                "Null town",
                "Null county",
                "CC1 1CC",
                None,
            ),
        ],
        _harmonised_schema(),
    )

    return df.withColumn("extraColumn", F.lit("ignore me"))


def _existing_curated_schema():
    return T.StructType(
        [
            T.StructField("eventId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("eventType", T.StringType(), True),
            T.StructField("eventName", T.StringType(), True),
            T.StructField("eventStatus", T.StringType(), True),
            T.StructField("isUrgent", T.BooleanType(), True),
            T.StructField("eventPublished", T.BooleanType(), True),
            T.StructField("eventStartDateTime", T.StringType(), True),
            T.StructField("eventEndDateTime", T.StringType(), True),
            T.StructField("notificationOfSiteVisit", T.StringType(), True),
            T.StructField("addressLine1", T.StringType(), True),
            T.StructField("addressLine2", T.StringType(), True),
            T.StructField("addressTown", T.StringType(), True),
            T.StructField("addressCounty", T.StringType(), True),
            T.StructField("addressPostcode", T.StringType(), True),
        ]
    )


def _existing_curated_df(spark):
    return spark.createDataFrame(
        [
            (
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
            )
        ],
        _existing_curated_schema(),
    )


class TestAppealEventCuratedProcess(ETLTestCase):
    def _run_process(self, spark, test_case: str, source_df):
        harmonised_table = f"{test_case}_appeal_event"
        curated_table = f"{test_case}_appeal_event"

        self.write_existing_table(
            spark,
            source_df,
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        with (
            mock.patch.object(
                AppealEventCuratedProcess,
                "SOURCE_TABLE",
                f"odw_harmonised_db.{harmonised_table}",
            ),
            mock.patch.object(
                AppealEventCuratedProcess,
                "OUTPUT_TABLE",
                curated_table,
            ),
        ):
            inst = AppealEventCuratedProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{curated_table}")

        return actual_df, result

    def _assert_curation(self, spark, actual_df, result):
        assert actual_df.columns == CURATED_COLUMNS
        assert "IsActive" not in actual_df.columns
        assert "extraColumn" not in actual_df.columns
        assert "notificationOfSitevisit" not in actual_df.columns
        assert "notificationOfSiteVisit" in actual_df.columns

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 1,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_selected_df = actual_df.select(
            "eventId",
            "caseReference",
            "eventType",
            "eventName",
            "eventStatus",
            "isUrgent",
            "eventPublished",
            "eventStartDateTime",
            "eventEndDateTime",
            "notificationOfSiteVisit",
            "addressLine1",
            "addressLine2",
            "addressTown",
            "addressCounty",
            "addressPostcode",
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "EVT-001",
                    "APP-001",
                    "hearing",
                    "Hearing event",
                    "scheduled",
                    True,
                    False,
                    "2025-01-01T09:00:00",
                    "2025-01-01T10:00:00",
                    "Y",
                    "Line 1",
                    "Line 2",
                    "Town",
                    "County",
                    "AA1 1AA",
                )
            ],
            actual_selected_df.schema,
        )

        assert_dataframes_equal(actual_selected_df, expected_df)

    def test__appeal_event_curated_process__run__creates_output_table_when_missing_like_legacy(self):
        test_case = "t_aecp_r_cotwm"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(spark, test_case, _harmonised_df(spark))

        self._assert_curation(spark, actual_df, result)

    def test__appeal_event_curated_process__run__overwrites_existing_output_table_like_legacy(self):
        test_case = "t_aecp_r_oeot"
        spark = PytestSparkSessionUtil().get_spark_session()

        curated_table = f"{test_case}_appeal_event"

        self.write_existing_table(
            spark,
            _existing_curated_df(spark),
            curated_table,
            "odw_curated_db",
            "odw-curated",
            curated_table,
            "overwrite",
        )

        actual_df, result = self._run_process(spark, test_case, _harmonised_df(spark))

        assert actual_df.where("eventId = 'OLD-EVENT'").count() == 0
        self._assert_curation(spark, actual_df, result)

    def test__appeal_event_curated_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        test_case = "t_aecp_r_ehs"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(spark, test_case, _empty_harmonised_df(spark))

        assert actual_df.count() == 0
        assert actual_df.columns == CURATED_COLUMNS

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 0,
            "update_count": 0,
            "delete_count": 0,
        }