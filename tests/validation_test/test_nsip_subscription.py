from tests.util.notebook_run_test_case import NotebookRunTestCase
import tests.util.constants as constants


class TestNsipSubscription(NotebookRunTestCase):
    def test_nsip_subscription_notebook(self):
        notebook_name = "py_unit_tests_nsip_subscription"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "nsip-subscription"
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
                "value": "sb_nsip_subscription"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_nsip_subscription"
            },
            "curated_table_name": {
                "type": "String",
                "value": "nsip_subscription"
            },
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
