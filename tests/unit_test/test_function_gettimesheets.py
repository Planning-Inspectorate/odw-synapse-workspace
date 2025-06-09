import pytest
import tests.util.pipelineutils as pipelineutils
import tests.util.constants as constants
import warnings

def test_function_gettimesheets(credential_name, azure_credential, synapse_endpoint: str):
    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    # run the pipeline
    pipelinename: str = "pln_run_function_app_unit_test"
    
    pipeline_raw_params = {
            "function_name": 
                 "gettimesheets"
    }       
    

    #run the pipeline
    pipeline_run_result = pipelineutils.run_and_observe_pipeline(credential_name, azure_credential, synapse_endpoint, pipelinename, pipeline_raw_params)
    assert pipeline_run_result == constants.PIPELINE_SUCCESS_STATUS
    print("test_function_gettimesheets_pipeline Completed")


@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")
