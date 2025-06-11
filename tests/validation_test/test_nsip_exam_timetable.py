import pytest
import tests.util.pipelineutils as pipelineutils
import tests.util.constants as constants
import warnings

def test_nsip_exam_timetable_notebook(credential_name, azure_credential, synapse_endpoint: str):

    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    # run the testing notebook
    notebookname: str = "py_unit_tests_nsip_exam_timetable"
    
    notebook_raw_params = {
        "notebook": notebookname,
        "parameters": {
            "entity_name": {
                "type": "String",
                "value": "nsip-exam-timetable"
            },
            "folder_name": {
                "type": "String",
                "value": "nsip-exam-timetable"
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
                "value": "sb_nsip_exam_timetable"
            },
            "hrm_table_name": {
                "type": "String",
                "value": "sb_nsip_exam_timetable"
            },
            "hrm_table_final": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
            "curated_table_name": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
            "primary_key": {
                "type": "String",
                "value": "caseReference"
            },
            "std_hzn_table_name": {
                "type": "String",
                "value": "horizon_examination_timetable"
            },
            "curated_db_migration_name": {
                "type": "String",
                "value": "odw_curated_migration_db"
            },
            "curated_migration_table_name": {
                "type": "String",
                "value": "nsip_exam_timetable"
            },
        }
    }
    notebook_raw_params.update(constants.SPARK_POOL_CONFIG)

    #run the notebook
    (notebook_run_result, exitMessage) = pipelineutils.run_and_observe_notebook(credential_name, azure_credential, synapse_endpoint, notebookname, notebook_raw_params)
    print("Notebook response *" +str(exitMessage) +"*")
    assert notebook_run_result == constants.NOTEBOOK_SUCCESS_STATUS 
    assert exitMessage == constants.NOTEBOOK_EXIT_CODE_SUCCESS
    print("test_nsip_exam_timetable Completed")

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")