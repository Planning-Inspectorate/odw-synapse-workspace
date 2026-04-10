from types import SimpleNamespace
from odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process import AppealAttributeMatrixStandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
import pyspark.sql.types as T
import pytest

pytestmark = pytest.mark.xfail(reason="Standardisation logic not implemented yet")


def test__appeal_attribute_matrix_standardisation_process__get_latest_ingestion_date__returns_latest_valid_date():
    spark = PytestSparkSessionUtil().get_spark_session()
    inst = AppealAttributeMatrixStandardisationProcess(spark)

    entry_names = [
        "2025-06-01/",
        "2025-06-15/",
        "backup/",
        "_temporary/",
        "2025-05-31/",
    ]

    actual = inst._get_latest_ingestion_date(entry_names)

    assert actual == "2025-06-15"


def test__appeal_attribute_matrix_standardisation_process__get_latest_ingestion_date__raises_when_no_valid_date_exists():
    spark = PytestSparkSessionUtil().get_spark_session()
    inst = AppealAttributeMatrixStandardisationProcess(spark)

    entry_names = [
        "backup/",
        "_temporary/",
        "latest/",
    ]

    try:
        inst._get_latest_ingestion_date(entry_names)
        assert False, "Expected FileNotFoundError to be raised"
    except FileNotFoundError as exc:
        assert "No YYYY-MM-DD folders found" in str(exc)


def test__appeal_attribute_matrix_standardisation_process__to_camel_case__matches_legacy_notebook():
    spark = PytestSparkSessionUtil().get_spark_session()
    inst = AppealAttributeMatrixStandardisationProcess(spark)

    assert inst._to_camel_case("Appeal Reference") == "appealReference"
    assert inst._to_camel_case("S78") == "s78"
    assert inst._to_camel_case("attribute") == "attribute"
    assert inst._to_camel_case("Rule 6 Party") == "rule6Party"


def test__appeal_attribute_matrix_standardisation_process__to_camel_case__handles_multiple_spaces_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()
    inst = AppealAttributeMatrixStandardisationProcess(spark)

    assert inst._to_camel_case("Appeal    Reference") == "appealReference"
    assert inst._to_camel_case("  Rule   6   Party  ") == "rule6Party"


def test__appeal_attribute_matrix_standardisation_process__load_data__uses_latest_valid_folder_and_correct_csv_options():
    expected_df = object()

    mock_reader = mock.Mock()
    mock_reader.option.return_value = mock_reader
    mock_reader.csv.return_value = expected_df

    mock_spark = mock.Mock()
    mock_spark.read = mock_reader

    inst = AppealAttributeMatrixStandardisationProcess(mock_spark)

    entries = [
        SimpleNamespace(name="2025-06-01/"),
        SimpleNamespace(name="backup/"),
        SimpleNamespace(name="2025-06-15/"),
    ]

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage/",
        ),
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.mssparkutils.fs.ls",
            return_value=entries,
        ),
    ):
        actual = inst.load_data()

    csv_path = mock_reader.csv.call_args[0][0]

    assert actual == {"raw_data": expected_df}
    assert csv_path == "abfss://odw-raw@test_storage/AppealAttributeMatrix/2025-06-15/appeal-attribute-matrix.csv"
    mock_reader.option.assert_any_call("header", True)
    mock_reader.option.assert_any_call("inferSchema", True)
    mock_reader.option.assert_any_call("ignoreLeadingWhiteSpace", True)
    mock_reader.option.assert_any_call("ignoreTrailingWhiteSpace", True)


def test__appeal_attribute_matrix_standardisation_process__load_data__raises_when_no_valid_date_folder_exists():
    mock_reader = mock.Mock()
    mock_spark = mock.Mock()
    mock_spark.read = mock_reader

    inst = AppealAttributeMatrixStandardisationProcess(mock_spark)

    entries = [
        SimpleNamespace(name="backup/"),
        SimpleNamespace(name="_temporary/"),
        SimpleNamespace(name="latest/"),
    ]

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage/",
        ),
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.mssparkutils.fs.ls",
            return_value=entries,
        ),
    ):
        try:
            inst.load_data()
            assert False, "Expected FileNotFoundError to be raised"
        except FileNotFoundError as exc:
            assert "No YYYY-MM-DD folders found under" in str(exc)


def test__appeal_attribute_matrix_standardisation_process__process__drops__c_columns_blank_columns_filters_invalid_attribute_rows_renames_columns_and_casts_s78_to_string():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001", 1, "drop-me", "drop-me-too"),
            ("", "APP-002", 0, "drop-me", "drop-me-too"),
            ("   ", "APP-003", 1, "drop-me", "drop-me-too"),
            (None, "APP-004", 0, "drop-me", "drop-me-too"),
            ('"', "APP-005", 1, "drop-me", "drop-me-too"),
            ("'", "APP-006", 0, "drop-me", "drop-me-too"),
            ("Green Belt", "APP-007", 0, "drop-me", "drop-me-too"),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
                T.StructField("_c0", T.StringType(), True),
                T.StructField("", T.StringType(), True),
            ]
        ),
    )

    with mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil"):
        inst = AppealAttributeMatrixStandardisationProcess(spark)
        data_to_write, result = inst.process(source_data={"raw_data": raw_data})

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    actual_rows = [tuple(row) for row in actual_df.orderBy("appealReference").collect()]

    expected_rows = [
        ("Housing Need", "APP-001", "1"),
        ("Green Belt", "APP-007", "0"),
    ]

    assert actual_df.columns == ["attribute", "appealReference", "s78"]
    assert "_c0" not in actual_df.columns
    assert "" not in actual_df.columns
    assert dict(actual_df.dtypes)["s78"] == "string"
    assert actual_rows == expected_rows
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_standardisation_process__process__does_not_fail_when_s78_column_is_missing():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001"),
            ("Green Belt", "APP-002"),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
            ]
        ),
    )

    with mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil"):
        inst = AppealAttributeMatrixStandardisationProcess(spark)
        data_to_write, result = inst.process(source_data={"raw_data": raw_data})

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    actual_rows = [tuple(row) for row in actual_df.orderBy("appealReference").collect()]

    expected_rows = [
        ("Housing Need", "APP-001"),
        ("Green Belt", "APP-002"),
    ]

    assert actual_df.columns == ["attribute", "appealReference"]
    assert actual_rows == expected_rows
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_standardisation_process__process__preserves_duplicate_valid_rows_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001", 1),
            ("Housing Need", "APP-001", 1),
            ("Green Belt", "APP-002", 0),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
            ]
        ),
    )

    with mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil"):
        inst = AppealAttributeMatrixStandardisationProcess(spark)
        data_to_write, result = inst.process(source_data={"raw_data": raw_data})

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    actual_rows = [tuple(row) for row in actual_df.orderBy("appealReference", "attribute").collect()]

    expected_rows = [
        ("Housing Need", "APP-001", "1"),
        ("Housing Need", "APP-001", "1"),
        ("Green Belt", "APP-002", "0"),
    ]

    assert actual_rows == expected_rows
    assert result.metadata.insert_count == 3


def test__appeal_attribute_matrix_standardisation_process__process__is_case_sensitive_for_attribute_column_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Valid Attribute", "APP-001", 1),
        ],
        T.StructType(
            [
                T.StructField("Attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
            ]
        ),
    )

    with mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil"):
        inst = AppealAttributeMatrixStandardisationProcess(spark)

        try:
            inst.process(source_data={"raw_data": raw_data})
            assert False, "Expected failure because legacy notebook filters on lowercase 'attribute' before renaming"
        except Exception:
            assert True
