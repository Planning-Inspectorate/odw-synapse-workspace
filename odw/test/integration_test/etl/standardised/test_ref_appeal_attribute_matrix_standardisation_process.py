import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process import AppealAttributeMatrixStandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
import pyspark.sql.types as T

pytestmark = pytest.mark.xfail(reason="Standardisation logic not implemented yet")


def test__appeal_attribute_matrix_standardisation_process__run__standardises_end_to_end_and_matches_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001", 1, "ignore", "drop-blank"),
            ("", "APP-002", 0, "ignore", "drop-blank"),
            ("   ", "APP-003", 1, "ignore", "drop-blank"),
            ("Green Belt", "APP-004", 0, "ignore", "drop-blank"),
            (None, "APP-005", 1, "ignore", "drop-blank"),
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

    source_data = {
        "raw_data": raw_data,
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = AppealAttributeMatrixStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    rows = [tuple(row) for row in actual_df.orderBy("appealReference").collect()]

    expected_rows = [
        ("Housing Need", "APP-001", "1"),
        ("Green Belt", "APP-004", "0"),
    ]

    assert actual_df.columns == ["attribute", "appealReference", "s78"]
    assert "_c0" not in actual_df.columns
    assert "" not in actual_df.columns
    assert dict(actual_df.dtypes)["s78"] == "string"
    assert rows == expected_rows
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_standardisation_process__run__handles_missing_s78_like_legacy():
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

    source_data = {
        "raw_data": raw_data,
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = AppealAttributeMatrixStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    rows = [tuple(row) for row in actual_df.orderBy("appealReference").collect()]

    expected_rows = [
        ("Housing Need", "APP-001"),
        ("Green Belt", "APP-002"),
    ]

    assert actual_df.columns == ["attribute", "appealReference"]
    assert rows == expected_rows
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__appeal_attribute_matrix_standardisation_process__run__filters_invalid_attribute_values_exactly_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            (None, "APP-001", 1),
            ("", "APP-002", 0),
            ("   ", "APP-003", 1),
            ('"', "APP-004", 0),
            ("'", "APP-005", 1),
            ("Valid Attribute", "APP-006", 1),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
            ]
        ),
    )

    source_data = {
        "raw_data": raw_data,
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = AppealAttributeMatrixStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = actual_df.collect()

    assert actual_df.columns == ["attribute", "appealReference", "s78"]
    assert len(rows) == 1
    assert rows[0]["attribute"] == "Valid Attribute"
    assert rows[0]["appealReference"] == "APP-006"
    assert rows[0]["s78"] == "1"
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1


def test__appeal_attribute_matrix_standardisation_process__run__preserves_duplicates_and_column_order_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001", 1, "drop"),
            ("Housing Need", "APP-001", 1, "drop"),
            ("Green Belt", "APP-002", 0, "drop"),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
                T.StructField("_c1", T.StringType(), True),
            ]
        ),
    )

    source_data = {
        "raw_data": raw_data,
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = AppealAttributeMatrixStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    rows = [tuple(row) for row in actual_df.orderBy("appealReference", "attribute").collect()]

    expected_rows = [
        ("Housing Need", "APP-001", "1"),
        ("Housing Need", "APP-001", "1"),
        ("Green Belt", "APP-002", "0"),
    ]

    assert actual_df.columns == ["attribute", "appealReference", "s78"]
    assert rows == expected_rows
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 3


def test__appeal_attribute_matrix_standardisation_process__run__handles_multiple_spaces_in_column_names_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_data = spark.createDataFrame(
        [
            ("Housing Need", "APP-001", 1),
        ],
        T.StructType(
            [
                T.StructField("attribute", T.StringType(), True),
                T.StructField("Appeal    Reference", T.StringType(), True),
                T.StructField("S78", T.IntegerType(), True),
            ]
        ),
    )

    source_data = {
        "raw_data": raw_data,
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = AppealAttributeMatrixStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = actual_df.collect()

    assert actual_df.columns == ["attribute", "appealReference", "s78"]
    assert len(rows) == 1
    assert rows[0]["attribute"] == "Housing Need"
    assert rows[0]["appealReference"] == "APP-001"
    assert rows[0]["s78"] == "1"
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
