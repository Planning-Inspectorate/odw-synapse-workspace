from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestNsipDocument(NotebookRunTestCase):
    def test_nsip_document_notebook(self):
        notebook_name = "py_unit_tests_nsip_document"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "nsip-document"
            },
            "folder_name": {
                "type": "String",
                "value": "nsip-document"
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
                "value": "sb_nsip_document"
            },
            "horizon_std_table_name": {
                "type": "String",
                "value": "document_meta_data"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_nsip_document"
            },
            "hrm_table_final": {
                "type": "String",
                "value": "nsip_document"
            },
            "curated_table_name": {
                "type": "String",
                "value": "nsip_document"
            },
            "std_hzn_table_name": {
                "type": "String",
                "value": "document_meta_data"
            },
            "curated_db_migration_name": {
                "type": "String",
                "value": "odw_curated_migration_db"
            },
            "curated_table_migration_name": {
                "type": "String",
                "value": "nsip_document"
            },
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
