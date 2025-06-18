from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestAppealDocument(NotebookRunTestCase):
    def test_appeal_document_notebook(self):
        notebook_name = "py_unit_tests_appeal_document"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "appeal-document"
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
                "value": "sb_appeal_document"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_appeal_document"
            },
            "hrm_table_final": {
                "type": "String",
                "value": "appeals_document_metadata"
            },
            "curated_table_name": {
                "type": "String",
                "value": "appeal_document"
            }
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
