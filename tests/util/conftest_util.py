from azure.identity import AzureCliCredential
import tests.util.constants as constants
from tests.util.config import TEST_CONFIG
import os


class ConftestUtil():
    @classmethod
    def get_credential_endpoint(cls) -> str:
        endpoint = os.getenv(constants.CREDENTIAL_ENVIRONMENT_NAME)
        if endpoint is None:
            endpoint = constants.CREDENTIAL_ENVIRONMENT_DEFAULT
        print(f"Credential Name is {endpoint}")
        return endpoint

    @classmethod
    def get_synapse_endpoint(cls) -> str:
        env = TEST_CONFIG["ENV"]
        return f"https://pins-synw-odw-{env}-uks.dev.azuresynapse.net/"

    @classmethod
    def get_azure_credential(cls, client_id: str = None, client_secret: str = None, tenant_id: str = None):
        return AzureCliCredential()
