from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestNsipExamTimetable(NotebookRunTestCase):
    def test_nsip_exam_timetable_notebook(self):
        notebook_name = "py_unit_tests_nsip_exam_timetable"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "nsip-exam-timetable"
            },
            "folder_name": {
                "type": "String",
                "value": "nsip-exam-timetable"
            },
            "std_db_name": {
                "type": "String",
                "value": "odw_standardised_db"
            },
            "hrm_db_name": {
                "type": "String",
                "value": "odw_harmonised_db"
            },
            "curated_db_name": {
                "type": "String",
                "value": "odw_curated_db"
            },
            "std_table_name": {
                "type": "String",
                "value": "sb_nsip_exam_timetable"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_nsip_exam_timetable"
            },
            "hrm_table_final": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
            "curated_table_name": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
            "primary_key": {
                "type": "String",
                "value": "caseReference"
            },
            "std_hzn_table_name": {
                "type": "String",
                "value": "horizon_examination_timetable"
            },
            "curated_db_migration_name": {
                "type": "String",
                "value": "odw_curated_migration_db"
            },
            "curated_migration_table_name": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
