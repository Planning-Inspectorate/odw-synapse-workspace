from tests.util.synapse_run_test_case import SynapseRunTestCase
import tests.util.constants as constants


class TestEntraId(SynapseRunTestCase):
    def test_entraid_pipeline(self):
        pipeline_name = "rel_1262_entra_id"
        pipeline_parameters = constants.SPARK_POOL_CONFIG

        pipeline_run_result = self.run_pipeline(pipeline_name, pipeline_parameters)
        assert pipeline_run_result["status"] == constants.PIPELINE_SUCCESS_STATUS

    def test_entraid_notebook(self):
        notebook_name = "py_unit_tests_has_appeals"
        notebook_parameters = {
            "entity_name": {
                "type": "String",
                "value": "entraid",
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
                "value": "entraid",
            },
            "hrm_table_name": {
                "type": "String",
                "value": "entraid",
            },
            "primary_key": {
                "type": "String",
                "value": "Id",
            },
        }

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        assert notebook_run_result["result"]["exitValue"] == constants.NOTEBOOK_EXIT_CODE_SUCCESS
