from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestServiceUser(NotebookRunTestCase):
    def test_service_user_notebook(self):
        notebook_name = "py_unit_tests_service_user"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "service-user",
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
                "value": "sb_service_user",
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_service_user",
            },
            "hrm_table_final": {
                "type": "String",
                "value": "service_user",
            },
            "service_user_appeal_table": {
                "type": "String",
                "value": "appeal_service_user",
            },
            "service_user_migration_table": {
                "type": "String",
                "value": "service_user"
            }
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
