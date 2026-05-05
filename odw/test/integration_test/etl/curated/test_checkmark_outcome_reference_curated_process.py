import mock
import pytest  # noqa: F401
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.curated.checkmark_outcome_reference_curated_process import (
    CheckmarkOutcomeReferenceCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


# Curated layer is a pass-through SELECT * from the harmonised table, so we
# use a small representative schema to exercise the code path.
_SAMPLE_COLUMNS = ["col_a", "col_b", "col_c"]
_SAMPLE_ROW = ("a", "b", "c")
_OTHER_ROW = ("x", "y", "z")

_MODULE = "odw.core.etl.transformation.curated.checkmark_outcome_reference_curated_process"


def _df(spark, rows):
    schema = StructType([StructField(c, StringType(), True) for c in _SAMPLE_COLUMNS])
    return spark.createDataFrame(rows, schema=schema)


def _run(spark, df_in):
    """Drive the process via run() with mocked load_data and write_data."""
    source_data = {"source_data": df_in}

    with (
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
        mock.patch(f"{_MODULE}.LoggingUtil") as mock_proc_logging,
    ):
        mock_etl_logging.return_value = mock.Mock()
        mock_proc_logging.return_value = mock.Mock()

        inst = CheckmarkOutcomeReferenceCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    return data_to_write, result


class TestCheckmarkOutcomeReferenceCuratedProcessIntegration(ETLTestCase):

    def test__outcome_reference_curated__run__passes_data_through_unchanged(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _run(spark, df_in)
        df_out = data_to_write["odw_curated_db.pbi_outcome_reference"]["data"]

        assert df_out.columns == _SAMPLE_COLUMNS
        assert df_out.collect()[0] == df_in.collect()[0]

    def test__outcome_reference_curated__run__write_metadata_targets_curated_layer(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _run(spark, df_in)
        write_config = data_to_write["odw_curated_db.pbi_outcome_reference"]

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert write_config["container_name"] == "odw-curated"
        assert write_config["table_name"] == "pbi_outcome_reference"
        assert write_config["database_name"] == "odw_curated_db"
        assert write_config["blob_path"] == "checkmarkdata/pbi_outcome_reference"

    def test__outcome_reference_curated__run__output_table_path_is_correct(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW])

        data_to_write, _ = _run(spark, df_in)
        out_keys = list(data_to_write.keys())

        assert out_keys == ["odw_curated_db.pbi_outcome_reference"]

    def test__outcome_reference_curated__run__multiple_rows_pass_through_with_correct_count(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW, _OTHER_ROW])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write["odw_curated_db.pbi_outcome_reference"]["data"]

        assert df_out.count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0
        assert result.metadata.activity_type == "CheckmarkOutcomeReferenceCuratedProcess"

    def test__outcome_reference_curated__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write["odw_curated_db.pbi_outcome_reference"]["data"]

        assert df_out.count() == 0
        assert data_to_write["odw_curated_db.pbi_outcome_reference"]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
