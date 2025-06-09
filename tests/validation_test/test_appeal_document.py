import pytest
import tests.util.constants as constants
import tests.util.pipelineutils as pipelineutils
import warnings

def test_appeal_document_notebook(credential_name, azure_credential, synapse_endpoint: str):

    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    # run the testing notebook
    notebookname: str = "py_unit_tests_appeal_document"
    notebook_raw_params = {
        "notebook": notebookname,
        "parameters": {
            "entity_name": {
               "type": "String",
               "value": "appeal-document",
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
               "value": "sb_appeal_document",
            },
            "hrm_table_name": {
               "type": "String",
               "value": "sb_appeal_document",
            },
            "hrm_table_final": {
               "type": "String",
               "value": "appeals_document_metadata",
            },
            "curated_table_name": {
               "type": "String",
               "value": "appeal_document",
            }
        }
    }
    notebook_raw_params.update(constants.SPARK_POOL_CONFIG)

    #run the notebook
    (notebook_run_result, exitMessage) = pipelineutils.run_and_observe_notebook(credential_name, azure_credential, synapse_endpoint, notebookname, notebook_raw_params)
    print("Notebook response *" +str(exitMessage) +"*")
    assert notebook_run_result == constants.NOTEBOOK_SUCCESS_STATUS 
    assert exitMessage == constants.NOTEBOOK_EXIT_CODE_SUCCESS
    print("test_appeal_document Completed")

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")
