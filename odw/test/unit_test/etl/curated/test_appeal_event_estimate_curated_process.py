import mock
import pyspark.sql.types as T
import pytest
from pyspark.sql import functions as F
from odw.core.etl.transformation.curated.appeal_event_estimate_curated_process import AppealEventEstimateCuratedProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

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


def _minimal_harmonised_schema():
    return T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.curated.curation_process.CurationProcess.__init__",
        return_value=None,
    ):
        inst = AppealEventEstimateCuratedProcess(spark)

    inst.spark = spark
    return inst


def _active_harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "AEE-001",
                "APP-001",
                "2 hours",
                "3 hours",
                "1 hour",
                "Y",
            )
        ],
        _harmonised_schema(),
    )


def _mixed_active_inactive_harmonised_df(spark):
    active = _active_harmonised_df(spark)

    inactive = (
        active.withColumn("id", F.lit("AEE-002"))
        .withColumn("caseReference", F.lit("APP-002"))
        .withColumn("preparationTime", F.lit("4 hours"))
        .withColumn("sittingTime", F.lit("5 hours"))
        .withColumn("reportingTime", F.lit("2 hours"))
        .withColumn("IsActive", F.lit("N"))
    )

    return active.unionByName(inactive)


def _minimal_missing_columns_df(spark):
    return spark.createDataFrame(
        [
            ("AEE-001", "APP-001", "Y"),
        ],
        _minimal_harmonised_schema(),
    )


def _duplicate_active_harmonised_df(spark):
    active = _active_harmonised_df(spark)
    return active.unionByName(active)


def _source_data(harmonised_data):
    return {"harmonised_appeal_event_estimate": harmonised_data}


class TestAppealEventEstimateCuratedProcess(SparkTestCase):
    @pytest.fixture(autouse=True)
    def _patch_storage_account(self):
        with mock.patch(
            "odw.core.util.util.Util.get_storage_account",
            return_value="test-storage.dfs.core.windows.net",
        ):
            yield

    def test__appeal_event_estimate_curated_process__process__filters_active_rows_and_projects_curated_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_mixed_active_inactive_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

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
                    "2 hours",
                    "3 hours",
                    "1 hour",
                )
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert "IsActive" not in df.columns

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_event_estimate_curated_process__process__adds_missing_curated_columns_as_null_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_minimal_missing_columns_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == CURATED_COLUMNS
        assert row["id"] == "AEE-001"
        assert row["caseReference"] == "APP-001"

        for col_name in CURATED_COLUMNS:
            if col_name not in {"id", "caseReference"}:
                assert row[col_name] is None

        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_process__process__excludes_inactive_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inactive_df = _mixed_active_inactive_harmonised_df(spark).where("IsActive = 'N'")

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(inactive_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert result.metadata.insert_count == 0

    def test__appeal_event_estimate_curated_process__process__empty_harmonised_input_writes_empty_curated_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_empty_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "parquet"
        assert result.metadata.insert_count == 0

    def test__appeal_event_estimate_curated_process__process__preserves_duplicate_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_duplicate_active_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where("id = 'AEE-001'").count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_event_estimate_curated_process__process__keeps_expected_write_config_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(_active_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_process__process__only_keeps_exact_uppercase_y_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        active = _active_harmonised_df(spark)

        lower_y = active.withColumn("IsActive", F.lit("y"))
        spaced_y = active.withColumn("IsActive", F.lit(" Y "))
        inactive = active.withColumn("IsActive", F.lit("N"))

        source_df = lower_y.unionByName(spaced_y).unionByName(inactive)

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert result.metadata.insert_count == 0

    def test__appeal_event_estimate_curated_process__process__ignores_extra_source_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _active_harmonised_df(spark).withColumn("extraColumn", F.lit("ignore me"))

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == CURATED_COLUMNS
        assert "extraColumn" not in df.columns
        assert result.metadata.insert_count == 1

    def test__appeal_event_estimate_curated_process__process__excludes_null_is_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _active_harmonised_df(spark).withColumn("IsActive", F.lit(None).cast(T.StringType()))

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert result.metadata.insert_count == 0

    def test__appeal_event_estimate_curated_process__process__preserves_null_selected_values_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [
                (
                    "AEE-001",
                    "APP-001",
                    None,
                    "3 hours",
                    None,
                    "Y",
                )
            ],
            _harmonised_schema(),
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["id"] == "AEE-001"
        assert row["caseReference"] == "APP-001"
        assert row["preparationTime"] is None
        assert row["sittingTime"] == "3 hours"
        assert row["reportingTime"] is None
        assert result.metadata.insert_count == 1
