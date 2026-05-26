from datetime import datetime
import mock
import pyspark.sql.types as T
import pytest
from pyspark.sql import functions as F
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_mipins_process import AppealEventEstimateCuratedMipinsProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

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


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.curated.curation_process.CurationProcess.__init__",
        return_value=None,
    ):
        inst = AppealEventEstimateCuratedMipinsProcess(spark)

    inst.spark = spark
    return inst


def _valid_odt_harmonised_df(spark):
    return spark.createDataFrame(
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
            )
        ],
        _harmonised_schema(),
    )


def _mixed_filter_harmonised_df(spark):
    valid = _valid_odt_harmonised_df(spark)

    non_odt = (
        valid.withColumn("AppealsEstimateEventID", F.lit("AEE-EVENT-002"))
        .withColumn("ID", F.lit("AEE-002"))
        .withColumn("caseReference", F.lit("APP-002"))
        .withColumn("ODTSourceSystem", F.lit("HORIZON"))
    )

    old_ingestion_date = (
        valid.withColumn("AppealsEstimateEventID", F.lit("AEE-EVENT-003"))
        .withColumn("ID", F.lit("AEE-003"))
        .withColumn("caseReference", F.lit("APP-003"))
        .withColumn("IngestionDate", F.lit("1899-12-31 23:59:59").cast(T.TimestampType()))
    )

    old_valid_to = (
        valid.withColumn("AppealsEstimateEventID", F.lit("AEE-EVENT-004"))
        .withColumn("ID", F.lit("AEE-004"))
        .withColumn("caseReference", F.lit("APP-004"))
        .withColumn("ValidTo", F.lit("1899-12-31 23:59:59").cast(T.TimestampType()))
    )

    inactive_but_valid = (
        valid.withColumn("AppealsEstimateEventID", F.lit("AEE-EVENT-005"))
        .withColumn("ID", F.lit("AEE-005"))
        .withColumn("caseReference", F.lit("APP-005"))
        .withColumn("ISActive", F.lit("N"))
    )

    null_dates_valid = (
        valid.withColumn("AppealsEstimateEventID", F.lit("AEE-EVENT-006"))
        .withColumn("ID", F.lit("AEE-006"))
        .withColumn("caseReference", F.lit("APP-006"))
        .withColumn("IngestionDate", F.lit(None).cast(T.TimestampType()))
        .withColumn("ValidTo", F.lit(None).cast(T.TimestampType()))
    )

    return (
        valid.unionByName(non_odt)
        .unionByName(old_ingestion_date)
        .unionByName(old_valid_to)
        .unionByName(inactive_but_valid)
        .unionByName(null_dates_valid)
    )


def _duplicate_valid_odt_harmonised_df(spark):
    valid = _valid_odt_harmonised_df(spark)
    return valid.unionByName(valid)


def _source_data(harmonised_data):
    return {"harmonised_appeal_event_estimate": harmonised_data}


class TestAppealEventEstimateCuratedMipinsProcess(SparkTestCase):
    @pytest.fixture(autouse=True)
    def _patch_storage_account(self):
        with mock.patch(
            "odw.core.util.util.Util.get_storage_account",
            return_value="test-storage.dfs.core.windows.net",
        ):
            yield

    def test__appeal_event_estimate_curated_mipins_process__process__filters_odt_and_valid_dates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_mixed_filter_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        actual_df = df.select("ID", "caseReference", "ODTSourceSystem", "ISActive").orderBy("ID")

        expected_df = spark.createDataFrame(
            [
                ("AEE-001", "APP-001", "ODT", "Y"),
                ("AEE-005", "APP-005", "ODT", "N"),
                ("AEE-006", "APP-006", "ODT", "Y"),
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert df.columns == CURATED_COLUMNS
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 3
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_event_estimate_curated_mipins_process__process__does_not_filter_inactive_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inactive_df = _valid_odt_harmonised_df(spark).withColumn("ISActive", F.lit("N"))

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(inactive_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.count() == 1
        assert row["ISActive"] == "N"
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_mipins_process__process__applies_distinct_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_duplicate_valid_odt_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_mipins_process__process__empty_harmonised_input_writes_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_empty_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "parquet"
        assert result.metadata.insert_count == 0

    def test__appeal_event_estimate_curated_mipins_process__process__ignores_extra_source_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _valid_odt_harmonised_df(spark).withColumn("extraColumn", F.lit("ignore me"))

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == CURATED_COLUMNS
        assert "extraColumn" not in df.columns
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_mipins_process__process__converts_utc_timestamp_columns_to_london_time_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _valid_odt_harmonised_df(spark)

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["IngestionDate"] == datetime(2025, 7, 1, 11, 0, 0)
        assert row["ValidTo"] == datetime(2025, 7, 2, 11, 0, 0)
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_mipins_process__process__keeps_expected_write_config_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_valid_odt_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config
        assert result.metadata.insert_count == 1
