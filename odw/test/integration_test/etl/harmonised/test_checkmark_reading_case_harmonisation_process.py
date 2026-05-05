import mock
import pytest  # noqa: F401
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.checkmark_reading_case_harmonisation_process import CheckmarkReadingCaseHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


_SOURCE_COLUMNS = ['case_reference', 'inspector_email', 'inspector_name', 'reader_email', 'reader_name', 'case_level', 'date_submitted_by_inspector', 'event_date', 'type_of_reading', 'procedure', 'case_target_date', 'reading_status', 'able_to_read_decision', 'advertisement', 'appeal_against_conditions', 'bespoke', 'case_team_officer_email', 'case_team_officer_name', 'costs_decision', 'green_belt', 'kiosk', 'linked', 'neighbourhood_plan', 'nsi_email', 'nsi_name', 'prior_approval', 'reason_for_delay', 'redetermination', 'secretary_of_state_report', 'spa_sac_eps', 'special_category_data', 'specialisms', 'telecommunications', 'urgent', 'amendments_received_notification_needed', 'able_to_check_amendments', 'amendments_cycles', 'amendments_target_date', 'amendments_timeliness_review_flag', 'date_amendments_checked', 'date_amendments_received', 'date_cleared', 'date_decision_sent_for_issue', 'date_read', 'date_read_started', 'decision_read', 'delayed_amendments_submission', 'delayed_submission', 'document_version', 'document_version_needed', 'final_decision_notification_needed', 'notification_sent_flag', 'on_hold_notification_needed', 'on_hold_status', 'reading_needed_flag', 'ready_for_issue', 'reason_for_amendments_delay', 'recall_notification_needed', 'replacement_decision_flag', 'return_count', 'revised_decision_notification_needed', 'skim_read', 'source', 'timeliness_review_flag', 'date_modified']

_SAMPLE_ROW = ('v0_c0_case_ref', 'v0_c1_inspecto', 'v0_c2_inspecto', 'v0_c3_reader_e', 'v0_c4_reader_n', 'v0_c5_case_lev', 'v0_c6_date_sub', 'v0_c7_event_da', 'v0_c8_type_of_', 'v0_c9_procedur', 'v0_c10_case_tar', 'v0_c11_reading_', 'v0_c12_able_to_', 'v0_c13_advertis', 'v0_c14_appeal_a', 'v0_c15_bespoke', 'v0_c16_case_tea', 'v0_c17_case_tea', 'v0_c18_costs_de', 'v0_c19_green_be', 'v0_c20_kiosk', 'v0_c21_linked', 'v0_c22_neighbou', 'v0_c23_nsi_emai', 'v0_c24_nsi_name', 'v0_c25_prior_ap', 'v0_c26_reason_f', 'v0_c27_redeterm', 'v0_c28_secretar', 'v0_c29_spa_sac_', 'v0_c30_special_', 'v0_c31_speciali', 'v0_c32_telecomm', 'v0_c33_urgent', 'v0_c34_amendmen', 'v0_c35_able_to_', 'v0_c36_amendmen', 'v0_c37_amendmen', 'v0_c38_amendmen', 'v0_c39_date_ame', 'v0_c40_date_ame', 'v0_c41_date_cle', 'v0_c42_date_dec', 'v0_c43_date_rea', 'v0_c44_date_rea', 'v0_c45_decision', 'v0_c46_delayed_', 'v0_c47_delayed_', 'v0_c48_document', 'v0_c49_document', 'v0_c50_final_de', 'v0_c51_notifica', 'v0_c52_on_hold_', 'v0_c53_on_hold_', 'v0_c54_reading_', 'v0_c55_ready_fo', 'v0_c56_reason_f', 'v0_c57_recall_n', 'v0_c58_replacem', 'v0_c59_return_c', 'v0_c60_revised_', 'v0_c61_skim_rea', 'v0_c62_source', 'v0_c63_timeline', 'v0_c64_date_mod')

_VARIANT_ROW = ('v0_c0_case_ref', 'v0_c1_inspecto', 'v0_c2_inspecto', 'v0_c3_reader_e', 'v0_c4_reader_n', 'v0_c5_case_lev', 'v0_c6_date_sub', 'v0_c7_event_da', 'v0_c8_type_of_', 'v0_c9_procedur', 'v0_c10_case_tar', 'v0_c11_reading_', 'v0_c12_able_to_', 'v0_c13_advertis', 'v0_c14_appeal_a', 'v0_c15_bespoke', 'v0_c16_case_tea', 'v0_c17_case_tea', 'v0_c18_costs_de', 'v0_c19_green_be', 'v0_c20_kiosk', 'v0_c21_linked', 'v0_c22_neighbou', 'v0_c23_nsi_emai', 'v0_c24_nsi_name', 'v0_c25_prior_ap', 'v0_c26_reason_f', 'v0_c27_redeterm', 'v0_c28_secretar', 'v0_c29_spa_sac_', 'v0_c30_special_', 'v0_c31_speciali', 'v0_c32_telecomm', 'v0_c33_urgent', 'v0_c34_amendmen', 'v0_c35_able_to_', 'v0_c36_amendmen', 'v0_c37_amendmen', 'v0_c38_amendmen', 'v0_c39_date_ame', 'v0_c40_date_ame', 'v0_c41_date_cle', 'v0_c42_date_dec', 'v0_c43_date_rea', 'v0_c44_date_rea', 'v0_c45_decision', 'v0_c46_delayed_', 'v0_c47_delayed_', 'v0_c48_document', 'v0_c49_document', 'v0_c50_final_de', 'v0_c51_notifica', 'v0_c52_on_hold_', 'v0_c53_on_hold_', 'v0_c54_reading_', 'v0_c55_ready_fo', 'v0_c56_reason_f', 'v0_c57_recall_n', 'v0_c58_replacem', 'v0_c59_return_c', 'v0_c60_revised_', 'v0_c61_skim_rea', 'v0_c62_source', 'v0_c63_timeline', 'v1_c64_date_mod')


def _source_schema():
    return StructType([StructField(c, StringType(), True) for c in _SOURCE_COLUMNS])


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_source_schema())


def _run(spark, df_in):
    """Drive the process via run() with mocked load_data and write_data."""
    source_data = {"source_data": df_in}

    with (
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
        mock.patch("odw.core.etl.transformation.harmonised.checkmark_reading_case_harmonisation_process.LoggingUtil") as mock_proc_logging,
    ):
        mock_etl_logging.return_value = mock.Mock()
        mock_proc_logging.return_value = mock.Mock()

        inst = CheckmarkReadingCaseHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    return data_to_write, result


class TestCheckmarkReadingCaseHarmonisationProcessIntegration(ETLTestCase):

    def test__reading_case_harmonisation__run__writes_single_row_with_rowid_and_ingestion_date(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW])

        data_to_write, result = _run(spark, df_in)

        write_config = data_to_write['odw_harmonised_db.reading_case']
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
        assert write_config["table_name"] == 'reading_case'
        assert write_config["database_name"] == "odw_harmonised_db"
        assert write_config["blob_path"] == 'reading_case'

        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0
        assert result.metadata.activity_type == "CheckmarkReadingCaseHarmonisationProcess"

    def test__reading_case_harmonisation__run__rowid_is_deterministic_for_same_input(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_a = _df(spark, [_SAMPLE_ROW])
        df_b = _df(spark, [_SAMPLE_ROW])

        out_a, _ = _run(spark, df_a)
        out_b, _ = _run(spark, df_b)

        rowid_a = out_a['odw_harmonised_db.reading_case']["data"].collect()[0]["RowID"]
        rowid_b = out_b['odw_harmonised_db.reading_case']["data"].collect()[0]["RowID"]
        assert rowid_a == rowid_b

    def test__reading_case_harmonisation__run__rowid_changes_when_any_column_changes(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_a = _df(spark, [_SAMPLE_ROW])
        df_b = _df(spark, [_VARIANT_ROW])

        out_a, _ = _run(spark, df_a)
        out_b, _ = _run(spark, df_b)

        rowid_a = out_a['odw_harmonised_db.reading_case']["data"].collect()[0]["RowID"]
        rowid_b = out_b['odw_harmonised_db.reading_case']["data"].collect()[0]["RowID"]
        assert rowid_a != rowid_b

    def test__reading_case_harmonisation__run__truncate_load_overwrites_with_multiple_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [_SAMPLE_ROW, _VARIANT_ROW])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.reading_case']["data"]

        assert df_out.count() == 2
        rowids = [r["RowID"] for r in df_out.collect()]
        assert len(set(rowids)) == 2
        assert result.metadata.insert_count == 2

    def test__reading_case_harmonisation__run__handles_null_columns_without_breaking_rowid(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        row_with_nulls = list(_SAMPLE_ROW)
        if len(row_with_nulls) > 1:
            row_with_nulls[1] = None
        df_in = _df(spark, [tuple(row_with_nulls)])

        data_to_write, _ = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.reading_case']["data"]
        row = df_out.collect()[0]

        assert row["RowID"] is not None
        assert len(row["RowID"]) == 32

    def test__reading_case_harmonisation__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df_in = _df(spark, [])

        data_to_write, result = _run(spark, df_in)
        df_out = data_to_write['odw_harmonised_db.reading_case']["data"]

        assert df_out.count() == 0
        assert data_to_write['odw_harmonised_db.reading_case']["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
