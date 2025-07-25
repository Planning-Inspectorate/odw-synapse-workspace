from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
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
        if client_id is None or client_secret is None or tenant_id is None:
            print(f"Credentials created from default")
            credentials = DefaultAzureCredential()
            return credentials
        print(f"Credentials created from parameters ")
        credentials = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id
        )
        return credentials
