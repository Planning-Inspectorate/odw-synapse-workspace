import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_process import AppealEventEstimateCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


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
            (
                "AEE-001",
                "APP-001",
                None,
                "3 hours",
                "1 hour",
                "Y",
            ),
            (
                "AEE-002",
                "APP-002",
                "4 hours",
                "5 hours",
                "2 hours",
                "N",
            ),
            (
                "AEE-003",
                "APP-003",
                "6 hours",
                "7 hours",
                None,
                None,
            ),
        ],
        _harmonised_schema(),
    )

    return df.withColumn("extraColumn", F.lit("ignore me"))


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


class TestAppealEventEstimateCuratedProcess(ETLTestCase):
    def test__appeal_event_estimate_curated_process__run__curates_active_harmonised_rows_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_harmonised_df(spark))

        inst = AppealEventEstimateCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert "IsActive" not in df.columns
        assert "extraColumn" not in df.columns
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 1,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_df = df.select(
            "id",
            "caseReference",
            "preparationTime",
            "sittingTime",
            "reportingTime",
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "AEE-001",
                    "APP-001",
                    None,
                    "3 hours",
                    "1 hour",
                )
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

    def test__appeal_event_estimate_curated_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_empty_harmonised_df(spark))

        inst = AppealEventEstimateCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"

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
        spark = PytestSparkSessionUtil().get_spark_session()

        minimal_df = spark.createDataFrame(
            [("AEE-001", "APP-001", "Y")],
            T.StructType(
                [
                    T.StructField("id", T.StringType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        source_data = _source_data(minimal_df)

        inst = AppealEventEstimateCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        df = mock_write.call_args[0][0][inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == CURATED_COLUMNS
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