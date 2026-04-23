from odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process import AppealAttributeMatrixCuratedProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import mock


def test__appeal_attribute_matrix_curated_process__process__filters_only_active_records_when_isactive_present():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [
            ("a", "Y"),
            ("b", "N"),
        ],
        ["attribute", "IsActive"],
    )

    std_data = spark.createDataFrame(
        [("a",), ("b",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 1
    assert df.collect()[0]["attribute"] == "a"
    assert result.metadata.insert_count == 1


def test__appeal_attribute_matrix_curated_process__process__isactive_branch_takes_precedence_over_latest_per_temp_pk():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [
            ("a-old", "pk1", "2025-01-01", "Y"),
            ("a-new", "pk1", "2025-02-01", "N"),
        ],
        ["attribute", "TEMP_PK", "IngestionDate", "IsActive"],
    )

    std_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, _ = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 1
    assert df.collect()[0]["attribute"] == "a-old"


def test__appeal_attribute_matrix_curated_process__process__selects_latest_record_per_temp_pk_when_no_isactive():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [
            ("a", "pk1", "2025-01-01"),
            ("a", "pk1", "2025-02-01"),
        ],
        ["attribute", "TEMP_PK", "IngestionDate"],
    )

    std_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 1
    assert df.collect()[0]["IngestionDate"] == "2025-02-01"
    assert result.metadata.insert_count == 1


def test__appeal_attribute_matrix_curated_process__process__passes_through_when_no_isactive_and_no_temp_pk():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [("a",), ("b",)],
        ["attribute"],
    )

    std_data = spark.createDataFrame(
        [("a",), ("b",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 2
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_curated_process__process__adds_missing_standardised_columns_as_string_type():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    std_data = spark.createDataFrame(
        [("a", "APP-001")],
        ["attribute", "appealReference"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, _ = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert "appealReference" in df.columns
    assert dict(df.dtypes)["appealReference"] == "string"
    assert df.collect()[0]["appealReference"] is None


def test__appeal_attribute_matrix_curated_process__process__orders_columns_std_first_then_extras():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [("a", "extra", "source-x", "2025-01-01", "Y")],
        ["attribute", "TEMP_PK", "ODTSourceSystem", "IngestionDate", "IsActive"],
    )

    std_data = spark.createDataFrame(
        [("a", "APP-001")],
        ["attribute", "appealReference"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, _ = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.columns == [
        "attribute",
        "appealReference",
        "TEMP_PK",
        "ODTSourceSystem",
        "IngestionDate",
        "IsActive",
    ]


def test__appeal_attribute_matrix_curated_process__process__casts_s78_to_string_if_present():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [("a", 1)],
        ["attribute", "s78"],
    )

    std_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, _ = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert dict(df.dtypes)["s78"] == "string"


def test__appeal_attribute_matrix_curated_process__process__uses_overwrite_write_mode_and_insert_count():
    spark = PytestSparkSessionUtil().get_spark_session()

    hrm_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    std_data = spark.createDataFrame(
        [("a",)],
        ["attribute"],
    )

    with mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil"):
        inst = AppealAttributeMatrixCuratedProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "harmonised_data": hrm_data,
                "standardised_data": std_data,
            }
        )

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
