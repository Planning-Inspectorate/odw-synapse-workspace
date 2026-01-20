from typing import Dict, Union


NOTEBOOK_SUCCESS_STATUS: str = "Succeeded"
PIPELINE_SUCCESS_STATUS: str = "Succeeded"
NOTEBOOK_EXIT_CODE_SUCCESS: str = '0'
SYNAPSE_ENDPOINT_ENVIRONMENT_VARIABLE: str = "SYNAPSE_ENDPOINT"
SYNAPSE_ENDPOINT_DEFAULT: str = "https://pins-synw-odw-dev-uks.dev.azuresynapse.net/"
CREDENTIAL_ENVIRONMENT_NAME: str = "CREDENTIAL_NAME"
CREDENTIAL_ENVIRONMENT_DEFAULT: str = "https://dev.azuresynapse.net/.default"
SPARK_POOL_CONFIG: Dict[str, Union[str, Dict[str, str]]] = {
    "sparkPool": "pinssynspodw34",
    "sessionOptions": {
        "driverMemory": "28g",
        "driverCores": 4,
        "executorMemory": "28g",
        "executorCores": 4,
        "numExecutors": 2,
        "runAsWorkspaceSystemIdentity": False
    }
}