import pytest
import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient
import warnings

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    print("#### Running Before and After")

def test_azure(azure_credential):
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    print("Testing Azure Credentials")
    credentials = azure_credential   
    subscription_client = SubscriptionClient(credentials)
    for item in subscription_client.subscriptions.list():
        print("subscription_id:" +item.subscription_id)
        print("tenant_id:" +item.tenant_id)
    assert credentials