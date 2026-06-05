import mock
import pyspark.sql.types as T
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_process import AppealEventEstimateCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil

CURATED_COLUMNS = [
    "id",
    "caseReference",
    "preparationTime",
    "sittingTime",
    "reportingTime",
]


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("preparationTime", T.StringType(), True),
            T.StructField("sittingTime", T.StringType(), True),
            T.StructField("reportingTime", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _harmonised_df(spark):
    df = spark.createDataFrame(
        [
            ("AEE-001", "APP-001", None, "3 hours", "1 hour", "Y"),
            ("AEE-002", "APP-002", "4 hours", "5 hours", "2 hours", "N"),
            ("AEE-003", "APP-003", "6 hours", "7 hours", None, None),
        ],
        _harmonised_schema(),
    )

    return df.withColumn("extraColumn", F.lit("ignore me"))


def _minimal_harmonised_df(spark):
    return spark.createDataFrame(
        [("AEE-001", "APP-001", "Y")],
        T.StructType(
            [
                T.StructField("id", T.StringType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("IsActive", T.StringType(), True),
            ]
        ),
    )


class TestAppealEventEstimateCuratedProcess(ETLTestCase):
    def _run_process(self, spark, test_case: str, source_df):
        harmonised_table = f"{test_case}_sb_appeal_event_estimate"
        curated_table = f"{test_case}_appeal_event_estimate"

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
                AppealEventEstimateCuratedProcess,
                "HARMONISED_TABLE",
                f"odw_harmonised_db.{harmonised_table}",
            ),
            mock.patch.object(
                AppealEventEstimateCuratedProcess,
                "OUTPUT_TABLE",
                curated_table,
            ),
        ):
            inst = AppealEventEstimateCuratedProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="appeal_event_estimate", orchestration_stage_name="curate")
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{curated_table}")

        return actual_df, result

    def test__appeal_event_estimate_curated_process__run__curates_active_harmonised_rows_end_to_end_like_legacy(self):
        test_case = "t_aeec_r_cahr"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(spark, test_case, _harmonised_df(spark))

        assert actual_df.columns == CURATED_COLUMNS
        assert "IsActive" not in actual_df.columns
        assert "extraColumn" not in actual_df.columns

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
            "id",
            "caseReference",
            "preparationTime",
            "sittingTime",
            "reportingTime",
        )

        expected_df = spark.createDataFrame(
            [
                ("AEE-001", "APP-001", None, "3 hours", "1 hour"),
            ],
            actual_selected_df.schema,
        )

        assert_dataframes_equal(actual_selected_df, expected_df)

    def test__appeal_event_estimate_curated_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        test_case = "t_aeec_r_ehs"
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

    def test__appeal_event_estimate_curated_process__run__adds_missing_columns_as_null_like_curated_framework(self):
        test_case = "t_aeec_r_amc"
        spark = PytestSparkSessionUtil().get_spark_session()

        actual_df, result = self._run_process(spark, test_case, _minimal_harmonised_df(spark))

        row = actual_df.collect()[0]

        assert actual_df.columns == CURATED_COLUMNS
        assert row["id"] == "AEE-001"
        assert row["caseReference"] == "APP-001"

        for col_name in CURATED_COLUMNS:
            if col_name not in {"id", "caseReference"}:
                assert row[col_name] is None

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 1,
            "update_count": 0,
            "delete_count": 0,
        }
