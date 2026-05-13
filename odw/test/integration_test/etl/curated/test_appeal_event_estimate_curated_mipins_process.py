from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_mipins_process import AppealEventEstimateCuratedMipinsProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.xfail(reason="Curated MIPINS logic not implemented yet")


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
                datetime(1899, 12, 31, 23, 59, 59),
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
                datetime(1899, 12, 31, 23, 59, 59),
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

    return df.withColumn("extraColumn", F.lit("ignore me"))


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


class TestAppealEventEstimateCuratedMipinsProcess(ETLTestCase):
    def test__appeal_event_estimate_curated_mipins_process__run__curates_mipins_rows_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_harmonised_df(spark))

        inst = AppealEventEstimateCuratedMipinsProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.columns == CURATED_COLUMNS
        assert "extraColumn" not in df.columns
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 2,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_df = df.select(
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
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

    def test__appeal_event_estimate_curated_mipins_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_empty_harmonised_df(spark))

        inst = AppealEventEstimateCuratedMipinsProcess(spark)

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