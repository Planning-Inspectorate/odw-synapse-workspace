import pytest
import datetime
import pytest
import pyodbc
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
import constants
import os


def pytest_addoption(parser):
    # input location at landing zone
    parser.addoption("--client_id", action="store")
    # input location at landing zone
    parser.addoption("--client_secret", action="store")
    # input location at landing zone
    parser.addoption("--tenant", action="store")

@pytest.fixture()
def pipeline_name(pytestconfig) -> str:
    return pytestconfig.getoption("pipeline")

@pytest.fixture()
def credential_name(pytestconfig) -> str:
    endpoint = os.getenv(constants.CREDENTIAL_ENVIRONMENT_NAME)
    if endpoint is None:
        endpoint = constants.CREDENTIAL_ENVIRONMENT_DEFAULT
    print(f"Credential Name is {endpoint}")
    return endpoint



@pytest.fixture()
def synapse_endpoint(pytestconfig) -> str:
    endpoint = os.getenv(constants.SYNAPSE_ENDPOINT_ENVIRONMENT_VARIABLE)
    if endpoint is None:
        endpoint = constants.SYNAPSE_ENDPOINT_DEFAULT
    print(f"Synapse Endpoint is {endpoint}")
    return endpoint

@pytest.fixture()
def azure_credential(pytestconfig):
    client_id = pytestconfig.getoption("client_id")
    client_secret = pytestconfig.getoption("client_secret")
    tenant_id = pytestconfig.getoption("tenant")
    if client_id is None or client_secret is None or tenant_id is None:
        print(f"Credentials created from default")
        credentials = DefaultAzureCredential()
        return credentials
    else:
        print(f"Credentials created from parameters ")
        credentials = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id)
        return credentials