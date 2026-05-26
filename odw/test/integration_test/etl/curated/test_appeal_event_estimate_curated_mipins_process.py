from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_mipins_process import AppealEventEstimateCuratedMipinsProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil

CURATED_COLUMNS = [
    "AppealsEstimateEventID",
    "ID",
    "caseReference",
    "preparationTime",
    "sittingTime",
    "reportingTime",
    "migrated",
    "ODTSourceSystem",
    "SourceSystemID",
    "IngestionDate",
    "ValidTo",
    "ROWID",
    "ISActive",
]


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("AppealsEstimateEventID", T.StringType(), True),
            T.StructField("ID", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("preparationTime", T.StringType(), True),
            T.StructField("sittingTime", T.StringType(), True),
            T.StructField("reportingTime", T.StringType(), True),
            T.StructField("migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("ROWID", T.StringType(), True),
            T.StructField("ISActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _harmonised_df(spark):
    df = spark.createDataFrame(
        [
            (
                "AEE-EVENT-001",
                "AEE-001",
                "APP-001",
                "2 hours",
                "3 hours",
                "1 hour",
                "N",
                "ODT",
                "SRC-001",
                datetime(2025, 7, 1, 10, 0, 0),
                datetime(2025, 7, 2, 10, 0, 0),
                "ROW-001",
                "Y",
            ),
            (
                "AEE-EVENT-002",
                "AEE-002",
                "APP-002",
                "4 hours",
                "5 hours",
                "2 hours",
                "N",
                "HORIZON",
                "SRC-002",
                datetime(2025, 7, 1, 10, 0, 0),
                datetime(2025, 7, 2, 10, 0, 0),
                "ROW-002",
                "Y",
            ),
            (
                "AEE-EVENT-003",
                "AEE-003",
                "APP-003",
                "6 hours",
                "7 hours",
                "3 hours",
                "N",
                "ODT",
                "SRC-003",
                None,
                datetime(2025, 7, 2, 10, 0, 0),
                "ROW-003",
                "Y",
            ),
            (
                "AEE-EVENT-004",
                "AEE-004",
                "APP-004",
                "8 hours",
                "9 hours",
                "4 hours",
                "N",
                "ODT",
                "SRC-004",
                datetime(2025, 7, 1, 10, 0, 0),
                None,
                "ROW-004",
                "Y",
            ),
            (
                "AEE-EVENT-005",
                "AEE-005",
                "APP-005",
                "10 hours",
                "11 hours",
                "5 hours",
                "Y",
                "ODT",
                "SRC-005",
                None,
                None,
                "ROW-005",
                "N",
            ),
        ],
        _harmonised_schema(),
    )

    pre1900 = F.lit("1899-12-31 23:59:59").cast(T.TimestampType())
    df = df.withColumn(
        "IngestionDate",
        F.when(F.col("AppealsEstimateEventID") == "AEE-EVENT-003", pre1900).otherwise(F.col("IngestionDate")),
    ).withColumn(
        "ValidTo",
        F.when(F.col("AppealsEstimateEventID") == "AEE-EVENT-004", pre1900).otherwise(F.col("ValidTo")),
    )

    return df.withColumn("extraColumn", F.lit("ignore me"))


def _existing_curated_schema():
    return T.StructType(
        [
            T.StructField("AppealsEstimateEventID", T.StringType(), True),
            T.StructField("ID", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("preparationTime", T.StringType(), True),
            T.StructField("sittingTime", T.StringType(), True),
            T.StructField("reportingTime", T.StringType(), True),
            T.StructField("migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("ROWID", T.StringType(), True),
            T.StructField("ISActive", T.StringType(), True),
        ]
    )


def _existing_curated_df(spark):
    return spark.createDataFrame(
        [
            (
                "OLD-EVENT",
                "OLD-ID",
                "OLD-CASE",
                "old prep",
                "old sitting",
                "old reporting",
                "N",
                "ODT",
                "OLD-SRC",
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 1, 2, 0, 0, 0),
                "OLD-ROW",
                "Y",
            )
        ],
        _existing_curated_schema(),
    )


class TestAppealEventEstimateCuratedMipinsProcess(ETLTestCase):
    @pytest.fixture(autouse=True)
    def _allow_ancient_datetimes(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        yield

    def _run_process(self, spark, test_case: str):
        harmonised_table = f"{test_case}_sb_appeal_event_estimate"

        self.write_existing_table(
            spark,
            _harmonised_df(spark),
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        with mock.patch.object(
            AppealEventEstimateCuratedMipinsProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{harmonised_table}",
        ):
            inst = AppealEventEstimateCuratedMipinsProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)

        actual_df = spark.table("odw_curated_db.appeal_event_estimate_curated_mipins")

        return actual_df, result

    def _assert_curation(self, spark, actual_df, result):
        assert actual_df.columns == CURATED_COLUMNS
        assert "extraColumn" not in actual_df.columns

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 2,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_selected_df = actual_df.select(
            "AppealsEstimateEventID",
            "ID",
            "caseReference",
            "preparationTime",
            "sittingTime",
            "reportingTime",
            "migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
            "ValidTo",
            "ROWID",
            "ISActive",
        ).orderBy("ID")

        expected_df = spark.createDataFrame(
            [
                (
                    "AEE-EVENT-001",
                    "AEE-001",
                    "APP-001",
                    "2 hours",
                    "3 hours",
                    "1 hour",
                    "N",
                    "ODT",
                    "SRC-001",
                    datetime(2025, 7, 1, 11, 0, 0),
                    datetime(2025, 7, 2, 11, 0, 0),
                    "ROW-001",
                    "Y",
                ),
                (
                    "AEE-EVENT-005",
                    "AEE-005",
                    "APP-005",
                    "10 hours",
                    "11 hours",
                    "5 hours",
                    "Y",
                    "ODT",
                    "SRC-005",
                    None,
                    None,
                    "ROW-005",
                    "N",
                ),
            ],
            actual_selected_df.schema,
        )

        assert_dataframes_equal(actual_selected_df, expected_df)

    def test__appeal_event_estimate_curated_mipins_process__run__creates_output_table_when_missing_like_legacy(self):
        test_case = "t_aeecm_r_cotwm"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(spark, test_case)

        self._assert_curation(spark, actual_df, result)

    def test__appeal_event_estimate_curated_mipins_process__run__overwrites_existing_output_table_like_legacy(self):
        test_case = "t_aeecm_r_oeot"
        spark = PytestSparkSessionUtil().get_spark_session()

        self.write_existing_table(
            spark,
            _existing_curated_df(spark),
            "appeal_event_estimate_curated_mipins",
            "odw_curated_db",
            "odw-curated",
            "appeal_event_estimate_curated_mipins",
            "overwrite",
        )

        actual_df, result = self._run_process(spark, test_case)

        assert actual_df.where("ID = 'OLD-ID'").count() == 0
        self._assert_curation(spark, actual_df, result)

    def test__appeal_event_estimate_curated_mipins_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        test_case = "t_aeecm_r_ehs"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_table = f"{test_case}_sb_appeal_event_estimate"

        self.write_existing_table(
            spark,
            _empty_harmonised_df(spark),
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        with mock.patch.object(
            AppealEventEstimateCuratedMipinsProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{harmonised_table}",
        ):
            inst = AppealEventEstimateCuratedMipinsProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)

        actual_df = spark.table("odw_curated_db.appeal_event_estimate_curated_mipins")

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
