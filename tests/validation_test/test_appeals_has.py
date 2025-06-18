from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestAppealsHas(NotebookRunTestCase):
    def test_appeals_has_notebook(self):
        notebook_name = "py_unit_tests_has_appeals"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "appeal-has",
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
                "value": "sb_appeal_has",
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_appeal_has",
            },
            "curated_table_name": {
                "type": "String",
                "value": "appeal_has",
            }
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
