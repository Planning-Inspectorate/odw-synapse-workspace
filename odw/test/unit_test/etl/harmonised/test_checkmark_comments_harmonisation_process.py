from contextlib import ExitStack
from odw.core.etl.transformation.harmonised.checkmark_comments_harmonisation_process import (
    CheckmarkCommentsHarmonisationProcess,
)
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
from pyspark.sql.types import StructType, StructField, StringType


_SAMPLE_ROW_FULL = (
    "id-001",
    "CASE/2025/0001",
    "review",
    "open",
    "alice@pins.gov.uk",
    "2025-01-01T10:00:00",
    "Looks good",
)

_SOURCE_COLUMNS = [
    "ID",
    "case_reference",
    "type",
    "state",
    "author",
    "timestamp",
    "comment",
]

_MODULE = "odw.core.etl.transformation.harmonised.checkmark_comments_harmonisation_process"


def _build_input_df(spark, rows):
    schema = StructType([StructField(c, StringType(), True) for c in _SOURCE_COLUMNS])
    return spark.createDataFrame(rows, schema=schema)


def _patched_run(spark, df_in):
    with ExitStack() as stack:
        stack.enter_context(mock.patch(f"{_MODULE}.LoggingUtil"))
        stack.enter_context(
            mock.patch(f"{_MODULE}.Util.get_storage_account", return_value="test_storage")
        )
        inst = CheckmarkCommentsHarmonisationProcess(spark)
        return inst.process(source_data={"source_data": df_in})


class TestCheckmarkCommentsHarmonisationProcess(SparkTestCase):

    def test__process__adds_ingestion_date_as_timestamp(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW_FULL])

        data_to_write, _ = _patched_run(spark, df_in)
        df_out = data_to_write[list(data_to_write.keys())[0]]["data"]

        assert "IngestionDate" in df_out.columns
        assert dict(df_out.dtypes)["IngestionDate"] == "timestamp"

    def test__process__adds_rowid_column(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW_FULL])

        data_to_write, _ = _patched_run(spark, df_in)
        df_out = data_to_write[list(data_to_write.keys())[0]]["data"]

        assert "RowID" in df_out.columns
        row = df_out.collect()[0]
        assert row["RowID"] is not None
        assert len(row["RowID"]) == 32

    def test__process__rowid_is_deterministic(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW_FULL])

        data_to_write_1, _ = _patched_run(spark, df_in)
        data_to_write_2, _ = _patched_run(spark, df_in)

        rowid_1 = data_to_write_1[list(data_to_write_1.keys())[0]]["data"].collect()[0]["RowID"]
        rowid_2 = data_to_write_2[list(data_to_write_2.keys())[0]]["data"].collect()[0]["RowID"]
        assert rowid_1 == rowid_2

    def test__process__rowid_changes_when_comment_changes(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        row_a = _SAMPLE_ROW_FULL
        row_b = list(_SAMPLE_ROW_FULL)
        row_b[6] = "Different comment"
        row_b = tuple(row_b)
        df_a = _build_input_df(spark, [row_a])
        df_b = _build_input_df(spark, [row_b])

        out_a, _ = _patched_run(spark, df_a)
        out_b, _ = _patched_run(spark, df_b)

        rowid_a = out_a[list(out_a.keys())[0]]["data"].collect()[0]["RowID"]
        rowid_b = out_b[list(out_b.keys())[0]]["data"].collect()[0]["RowID"]
        assert rowid_a != rowid_b

    def test__process__handles_null_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        row_with_nulls = list(_SAMPLE_ROW_FULL)
        row_with_nulls[6] = None
        df_in = _build_input_df(spark, [tuple(row_with_nulls)])

        data_to_write, _ = _patched_run(spark, df_in)
        df_out = data_to_write[list(data_to_write.keys())[0]]["data"]
        row = df_out.collect()[0]
        assert row["RowID"] is not None
        assert len(row["RowID"]) == 32

    def test__process__data_to_write_has_overwrite_mode(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW_FULL])

        data_to_write, _ = _patched_run(spark, df_in)
        write_metadata = data_to_write[list(data_to_write.keys())[0]]

        assert write_metadata["write_mode"] == "overwrite"
        assert write_metadata["storage_kind"] == "ADLSG2-Table"
        assert write_metadata["file_format"] == "delta"
        assert write_metadata["container_name"] == "odw-harmonised"
        assert write_metadata["blob_path"] == "comments"

    def test__process__returns_success_result_with_insert_count(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW_FULL, _SAMPLE_ROW_FULL])

        _, result = _patched_run(spark, df_in)

        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0
        assert result.metadata.activity_type == "CheckmarkCommentsHarmonisationProcess"
