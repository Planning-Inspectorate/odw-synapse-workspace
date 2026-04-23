from odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process import (
    AppealAttributeMatrixHarmonisationProcess,
)
from odw.test.util.session_util import PytestSparkSessionUtil


def test__appeal_attribute_matrix_harmonisation_process__process__trims_all_string_columns_and_normalises_attribute():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("  Housing Need  ", " APP-001 ", " 1 ", "  custom-source  ", " Y "),
        ],
        ["attribute", "appealReference", "s78", "ODTSourceSystem", "IsActive"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, result = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    row = df.collect()[0]

    # string normalisation
    assert row["attribute"] == "housing need"
    assert row["appealReference"] == "APP-001"
    assert row["s78"] == "1"

    # observed harmonised behaviour
    assert row["ODTSourceSystem"] == "X"
    assert row["IsActive"] is None

    assert result.metadata.insert_count == 1


def test__appeal_attribute_matrix_harmonisation_process__process__does_not_generate_temp_pk():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("  Housing Need  ",),
        ],
        ["attribute"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    # TEMP_PK is not produced at harmonised layer
    assert "TEMP_PK" not in df.columns


def test__appeal_attribute_matrix_harmonisation_process__process__adds_default_columns_when_missing():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [("housing need",)],
        ["attribute"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    row = df.collect()[0]

    assert row["ODTSourceSystem"] == "X"
    assert row["IsActive"] is None
    assert row["IngestionDate"] is not None


def test__appeal_attribute_matrix_harmonisation_process__process__overrides_existing_odt_source_system_and_isactive():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need", "ManualLoad", "N"),
        ],
        ["attribute", "ODTSourceSystem", "IsActive"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    row = df.collect()[0]

    # harmonised overrides upstream metadata
    assert row["ODTSourceSystem"] == "X"
    assert row["IsActive"] is None


def test__appeal_attribute_matrix_harmonisation_process__process__casts_existing_ingestion_date_to_timestamp():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need", "2025-01-01"),
        ],
        ["attribute", "IngestionDate"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert dict(df.dtypes)["IngestionDate"] == "timestamp"


def test__appeal_attribute_matrix_harmonisation_process__process__keeps_original_column_order_and_appends_extras():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need", "APP-001"),
        ],
        ["attribute", "appealReference"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.columns == [
        "attribute",
        "appealReference",
        "ODTSourceSystem",
        "IngestionDate",
        "IsActive",
    ]


def test__appeal_attribute_matrix_harmonisation_process__process__casts_s78_to_string_if_present():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need", 1),
        ],
        ["attribute", "s78"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert dict(df.dtypes)["s78"] == "string"


def test__appeal_attribute_matrix_harmonisation_process__process__preserves_duplicates_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need",),
            ("housing need",),
        ],
        ["attribute"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, result = inst.process(source_data={"standardised_data": std_data})

    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 2
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_harmonisation_process__process__uses_overwrite_write_mode():
    spark = PytestSparkSessionUtil().get_spark_session()

    std_data = spark.createDataFrame(
        [
            ("housing need",),
        ],
        ["attribute"],
    )

    inst = AppealAttributeMatrixHarmonisationProcess(spark)
    data_to_write, result = inst.process(source_data={"standardised_data": std_data})

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
