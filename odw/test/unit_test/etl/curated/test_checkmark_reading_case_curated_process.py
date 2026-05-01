from contextlib import ExitStack
from odw.core.etl.transformation.curated.checkmark_reading_case_curated_process import (
    CheckmarkReadingCaseCuratedProcess,
)
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
from pyspark.sql.types import StructType, StructField, StringType


_SAMPLE_COLUMNS = ["col_a", "col_b", "col_c"]
_SAMPLE_ROW = ("a", "b", "c")

_MODULE = "odw.core.etl.transformation.curated.checkmark_reading_case_curated_process"


def _build_input_df(spark, rows):
    schema = StructType([StructField(c, StringType(), True) for c in _SAMPLE_COLUMNS])
    return spark.createDataFrame(rows, schema=schema)


def _patched_run(spark, df_in):
    with ExitStack() as stack:
        stack.enter_context(mock.patch(f"{_MODULE}.LoggingUtil"))
        stack.enter_context(
            mock.patch(f"{_MODULE}.Util.get_storage_account", return_value="test_storage")
        )
        inst = CheckmarkReadingCaseCuratedProcess(spark)
        return inst.process(source_data={"source_data": df_in})


class TestCheckmarkReadingCaseCuratedProcess(SparkTestCase):

    def test__process__passes_data_through_unchanged(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _patched_run(spark, df_in)
        df_out = data_to_write[list(data_to_write.keys())[0]]["data"]

        # Curated is a pass-through, so columns and rows should be identical
        assert df_out.columns == _SAMPLE_COLUMNS
        assert df_out.collect()[0] == df_in.collect()[0]

    def test__process__data_to_write_has_correct_metadata(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _patched_run(spark, df_in)
        out_key = list(data_to_write.keys())[0]
        write_metadata = data_to_write[out_key]

        assert write_metadata["write_mode"] == "overwrite"
        assert write_metadata["storage_kind"] == "ADLSG2-Table"
        assert write_metadata["file_format"] == "delta"
        assert write_metadata["container_name"] == "odw-curated"
        assert write_metadata["blob_path"] == "checkmarkdata/pbi_reading_case"
        assert write_metadata["table_name"] == "pbi_reading_case"
        assert write_metadata["database_name"] == "odw_curated_db"

    def test__process__output_table_path_is_correct(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _patched_run(spark, df_in)
        out_key = list(data_to_write.keys())[0]

        assert out_key == "odw_curated_db.pbi_reading_case"

    def test__process__returns_success_result_with_insert_count(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [_SAMPLE_ROW, _SAMPLE_ROW])

        _, result = _patched_run(spark, df_in)

        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0
        assert result.metadata.activity_type == "CheckmarkReadingCaseCuratedProcess"

    def test__process__handles_empty_dataframe(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _build_input_df(spark, [])

        _, result = _patched_run(spark, df_in)

        assert result.metadata.insert_count == 0
