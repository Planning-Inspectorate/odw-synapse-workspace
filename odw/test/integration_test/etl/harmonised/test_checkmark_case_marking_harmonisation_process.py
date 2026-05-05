import mock
import pytest  # noqa: F401
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.checkmark_case_marking_harmonisation_process import CheckmarkCaseMarkingHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


_SOURCE_COLUMNS = ['ID', 'case_reference', 'overall_mark', 'amendments_timeliness_mark', 'complexity', 'conditions_mark', 'conditions_detail', 'coverage_mark', 'coverage_detail', 'ground_a_mark', 'ground_a_iit_level', 'invalid_nullity', 'invalid_nullity_mark', 'legal_grounds_mark', 'legal_grounds_considered', 'non_legal_grounds_mark', 'non_legal_grounds_considered', 'other_grounds_affecting_complexity', 'outcome', 'overall_case_level_for_iit_progression', 'presentation_accuracy_mark', 'presentation_accuracy_detail', 'structure_reasoning_mark', 'structure_reasoning_detail', 'timeliness_mark', 'reading_complete_notification_needed']

_SAMPLE_ROW = ('v0_c0_ID', 'v0_c1_case_ref', 'v0_c2_overall_', 'v0_c3_amendmen', 'v0_c4_complexi', 'v0_c5_conditio', 'v0_c6_conditio', 'v0_c7_coverage', 'v0_c8_coverage', 'v0_c9_ground_a', 'v0_c10_ground_a', 'v0_c11_invalid_', 'v0_c12_invalid_', 'v0_c13_legal_gr', 'v0_c14_legal_gr', 'v0_c15_non_lega', 'v0_c16_non_lega', 'v0_c17_other_gr', 'v0_c18_outcome', 'v0_c19_overall_', 'v0_c20_presenta', 'v0_c21_presenta', 'v0_c22_structur', 'v0_c23_structur', 'v0_c24_timeline', 'v0_c25_reading_')

_VARIANT_ROW = ('v0_c0_ID', 'v0_c1_case_ref', 'v0_c2_overall_', 'v0_c3_amendmen', 'v0_c4_complexi', 'v0_c5_conditio', 'v0_c6_conditio', 'v0_c7_coverage', 'v0_c8_coverage', 'v0_c9_ground_a', 'v0_c10_ground_a', 'v0_c11_invalid_', 'v0_c12_invalid_', 'v0_c13_legal_gr', 'v0_c14_legal_gr', 'v0_c15_non_lega', 'v0_c16_non_lega', 'v0_c17_other_gr', 'v0_c18_outcome', 'v0_c19_overall_', 'v0_c20_presenta', 'v0_c21_presenta', 'v0_c22_structur', 'v0_c23_structur', 'v0_c24_timeline', 'v1_c25_reading_')


def _source_schema():
    return StructType([StructField(c, StringType(), True) for c in _SOURCE_COLUMNS])


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_source_schema())


def _run(spark, df_in):
    """Drive the process via run() with mocked load_data and write_data."""
    source_data = {"source_data": df_in}

    with (
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
        mock.patch("odw.core.etl.transformation.harmonised.checkmark_case_marking_harmonisation_process.LoggingUtil") as mock_proc_logging,
    ):
        mock_etl_logging.return_value = mock.Mock()
        mock_proc_logging.return_value = mock.Mock()

        inst = CheckmarkCaseMarkingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    return data_to_write, result


class TestCheckmarkCaseMarkingHarmonisationProcessIntegration(ETLTestCase):

    def test__case_marking_harmonisation__run__writes_single_row_with_rowid_and_ingestion_date(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW])

        data_to_write, result = _run(spark, df_in)

        write_config = data_to_write['odw_harmonised_db.case_marking']
        df_out = write_config["data"]
        rows = df_out.collect()

        assert len(rows) == 1
        assert "RowID" in df_out.columns
        assert "IngestionDate" in df_out.columns
        assert rows[0]["RowID"] is not None
        assert len(rows[0]["RowID"]) == 32
        assert dict(df_out.dtypes)["IngestionDate"] == "timestamp"

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert write_config["container_name"] == "odw-harmonised"
        assert write_config["table_name"] == 'case_marking'
        assert write_config["database_name"] == "odw_harmonised_db"
        assert write_config["blob_path"] == 'case_marking'

        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0
        assert result.metadata.activity_type == "CheckmarkCaseMarkingHarmonisationProcess"

    def test__case_marking_harmonisation__run__rowid_is_deterministic_for_same_input(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_a = _df(spark, [_SAMPLE_ROW])
        df_b = _df(spark, [_SAMPLE_ROW])

        out_a, _ = _run(spark, df_a)
        out_b, _ = _run(spark, df_b)

        rowid_a = out_a['odw_harmonised_db.case_marking']["data"].collect()[0]["RowID"]
        rowid_b = out_b['odw_harmonised_db.case_marking']["data"].collect()[0]["RowID"]
        assert rowid_a == rowid_b

    def test__case_marking_harmonisation__run__rowid_changes_when_any_column_changes(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_a = _df(spark, [_SAMPLE_ROW])
        df_b = _df(spark, [_VARIANT_ROW])

        out_a, _ = _run(spark, df_a)
        out_b, _ = _run(spark, df_b)

        rowid_a = out_a['odw_harmonised_db.case_marking']["data"].collect()[0]["RowID"]
        rowid_b = out_b['odw_harmonised_db.case_marking']["data"].collect()[0]["RowID"]
        assert rowid_a != rowid_b

    def test__case_marking_harmonisation__run__truncate_load_overwrites_with_multiple_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW, _VARIANT_ROW])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.case_marking']["data"]

        assert df_out.count() == 2
        rowids = [r["RowID"] for r in df_out.collect()]
        assert len(set(rowids)) == 2
        assert result.metadata.insert_count == 2

    def test__case_marking_harmonisation__run__handles_null_columns_without_breaking_rowid(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        row_with_nulls = list(_SAMPLE_ROW)
        if len(row_with_nulls) > 1:
            row_with_nulls[1] = None
        df_in = _df(spark, [tuple(row_with_nulls)])

        data_to_write, _ = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.case_marking']["data"]
        row = df_out.collect()[0]

        assert row["RowID"] is not None
        assert len(row["RowID"]) == 32

    def test__case_marking_harmonisation__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.case_marking']["data"]

        assert df_out.count() == 0
        assert data_to_write['odw_harmonised_db.case_marking']["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
