import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.core.etl.transformation.curated.appeal_event_curated_process import AppealEventCuratedProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


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


def _minimal_harmonised_schema():
    return T.StructType(
        [
            T.StructField("eventId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _active_harmonised_df(spark):
    return spark.createDataFrame(
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
            )
        ],
        _harmonised_schema(),
    )


def _mixed_active_inactive_harmonised_df(spark):
    active = _active_harmonised_df(spark)

    inactive = (
        active.withColumn("eventId", F.lit("EVT-002"))
        .withColumn("caseReference", F.lit("APP-002"))
        .withColumn("eventName", F.lit("Inactive event"))
        .withColumn("IsActive", F.lit("N"))
    )

    null_active = (
        active.withColumn("eventId", F.lit("EVT-003"))
        .withColumn("caseReference", F.lit("APP-003"))
        .withColumn("eventName", F.lit("Null active event"))
        .withColumn("IsActive", F.lit(None).cast(T.StringType()))
    )

    return active.unionByName(inactive).unionByName(null_active)


def _minimal_missing_columns_df(spark):
    return spark.createDataFrame(
        [
            ("EVT-001", "APP-001", "Y"),
        ],
        _minimal_harmonised_schema(),
    )


def _duplicate_active_harmonised_df(spark):
    active = _active_harmonised_df(spark)
    return active.unionByName(active)


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.curated.curation_process.CurationProcess.__init__",
        return_value=None,
    ):
        inst = AppealEventCuratedProcess(spark)

    inst.spark = spark
    return inst


class TestAppealEventCuratedProcess(SparkTestCase):
    def test__appeal_event_curated_process__process__filters_active_rows_and_projects_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_mixed_active_inactive_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.columns == CURATED_COLUMNS
        assert "IsActive" not in df.columns
        assert "notificationOfSitevisit" not in df.columns
        assert "notificationOfSiteVisit" in df.columns

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config

        actual_df = df.select(
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
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_event_curated_process__process__only_keeps_exact_uppercase_y_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        active = _active_harmonised_df(spark)

        lower_y = active.withColumn("IsActive", F.lit("y"))
        spaced_y = active.withColumn("IsActive", F.lit(" Y "))
        inactive = active.withColumn("IsActive", F.lit("N"))
        null_active = active.withColumn("IsActive", F.lit(None).cast(T.StringType()))

        source_df = lower_y.unionByName(spaced_y).unionByName(inactive).unionByName(null_active)

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert result.metadata.insert_count == 0

    def test__appeal_event_curated_process__process__adds_missing_columns_as_null_like_curated_framework(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_minimal_missing_columns_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == CURATED_COLUMNS
        assert row["eventId"] == "EVT-001"
        assert row["caseReference"] == "APP-001"

        for col_name in CURATED_COLUMNS:
            if col_name not in {"eventId", "caseReference"}:
                assert row[col_name] is None

        assert result.metadata.insert_count == 1

    def test__appeal_event_curated_process__process__empty_harmonised_input_writes_empty_curated_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_empty_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "parquet"
        assert result.metadata.insert_count == 0

    def test__appeal_event_curated_process__process__preserves_duplicate_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_duplicate_active_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where("eventId = 'EVT-001'").count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_event_curated_process__process__ignores_extra_source_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _active_harmonised_df(spark).withColumn("extraColumn", F.lit("ignore me"))

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == CURATED_COLUMNS
        assert "extraColumn" not in df.columns
        assert result.metadata.insert_count == 1