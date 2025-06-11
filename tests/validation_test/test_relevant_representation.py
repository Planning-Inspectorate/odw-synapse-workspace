import pytest
import tests.util.pipelineutils as pipelineutils
import tests.util.constants as constants
import warnings

def test_relevant_representation_notebook(credential_name, azure_credential, synapse_endpoint: str):

    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    # run the testing notebook
    notebookname: str = "py_unit_tests_relevant_representation"
    
    notebook_raw_params = {
        "notebook": notebookname,
        "parameters": {
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
    }
    notebook_raw_params.update(constants.SPARK_POOL_CONFIG)

    #run the notebook
    (notebook_run_result, exitMessage) = pipelineutils.run_and_observe_notebook(credential_name, azure_credential, synapse_endpoint, notebookname, notebook_raw_params)
    print("Notebook response *" +str(exitMessage) +"*")
    assert notebook_run_result == constants.NOTEBOOK_SUCCESS_STATUS 
    assert exitMessage == constants.NOTEBOOK_EXIT_CODE_SUCCESS
    print("test_relevant_representation Completed")

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")
