from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestNsipS51Advice(NotebookRunTestCase):
    def test_nsip_s51_advice_notebook(self):
        notebook_name = "py_unit_tests_nsip_s51_advice"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "s51-advice"
            },
            "folder_name": {
                "type": "String",
                "value": "s51-advice"
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
                "value": "sb_s51_advice"
            },
            "horizon_std_table_name": {
                "type": "String",
                "value": "horizon_nsip_advice"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_s51_advice"
            },
            "hrm_table_final": {
                "type": "String",
                "value": "nsip_s51_advice"
            },
            "curated_table_name": {
                "type": "String",
                "value": "s51_advice"
            },
            "curated_db_migration_name": {
                "type": "String",
                "value": "odw_curated_migration_db"
            },
            "curated_table_migration_name": {
                "type": "String",
                "value": "s51_advice"
            },
            "primary_key": {
                "type": "String",
                "value": "adviceId"
            },
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
