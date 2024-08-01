import pytest
import pipelineutils
import constants
import warnings

def test_nsip_s51_advice_notebook(credential_name, azure_credential, synapse_endpoint: str):

    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    # run the testing notebook
    notebookname: str = "py_unit_tests_nsip_s51_advice"
    
    notebook_raw_params = {
        "sparkPool": "pinssynspodw",
        "notebook": notebookname,
        "sessionOptions": {
            "driverMemory": "28g",
            "driverCores": 4,
            "executorMemory": "28g",
            "executorCores": 4,
            "numExecutors": 2,
            "runAsWorkspaceSystemIdentity": False
        }
    }

    #run the notebook
    (notebook_run_result, exitMessage) = pipelineutils.run_and_observe_notebook(credential_name, azure_credential, synapse_endpoint, notebookname, notebook_raw_params)
    print("Notebook response *" +str(exitMessage) +"*")
    assert notebook_run_result == constants.NOTEBOOK_SUCCESS_STATUS 
    assert exitMessage == constants.NOTEBOOK_EXIT_CODE_SUCCESS
    print("test_nsip_s51_advice Completed")

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")