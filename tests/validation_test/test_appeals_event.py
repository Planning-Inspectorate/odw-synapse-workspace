from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestAppealsEvent(NotebookRunTestCase):
    def test_appeals_event_notebook(self):
        notebook_name = "py_unit_tests_appeals_events"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "appeal-event",
            },
            "std_db_name": {
                "type": "String",
                "value": "odw_standardised_db",
            },
            "hrm_db_name": {
                "type": "String",
                "value": "odw_harmonised_db",
            },
            "curated_db_name": {
                "type": "String",
                "value": "odw_curated_db",
            },
            "std_table_name": {
                "type": "String",
                "value": "sb_appeal_event",
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_appeal_event",
            },
            "curated_table_name": {
                "type": "String",
                "value": "appeal_event",
            }
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
