from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestRelevantRepresentation(NotebookRunTestCase):
    def test_relevant_representation_notebook(self):
        notebook_name = "py_unit_tests_relevant_representation"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "nsip-representation",
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
            "curated_migration_db_name": {
                "type": "String",
                "value": "odw_curated_migration_db",
            },
            "std_table_name": {
                "type": "String",
                "value": "sb_nsip_representation",
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_nsip_representation",
            },
            "hrm_table_final": {
                "type": "String",
                "value": "nsip_representation",
            },
            "curated_table_name": {
                "type": "String",
                "value": "nsip_representation",
            },
            "std_hzn_table_name": {
                "type": "String",
                "value": "horizon_nsip_relevant_representation",
            },
            "curated_migration_table_name": {
                "type": "String",
                "value": "nsip_representation",
            }
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
