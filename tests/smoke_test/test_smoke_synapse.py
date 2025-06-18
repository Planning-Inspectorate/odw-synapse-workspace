from tests.util.config import TEST_CONFIG
from tests.util.azure_management_test_case import AzureManagementTestCase
from tests.util.synapse_test_case import SynapseTestCase
from typing import Dict, Any
import requests
import json


class TestSmokeSynapse(AzureManagementTestCase, SynapseTestCase):
    WORKSPACE_NAME = f"pins-synw-odw-{TEST_CONFIG['ENV'].lower()}-uks"

    def _get_synapse_workspace(self, workspace_name: str) -> Dict[str, Any]:
        subscription_id = TEST_CONFIG["SUBSCRIPTION_ID"]
        resource_group_name = f"pins-rg-data-odw-{TEST_CONFIG['ENV'].lower()}-uks"
        workspace_url = (
            f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/"
            f"providers/Microsoft.Synapse/workspaces/{workspace_name}?api-version=2021-06-01"
        )
        request_headers = {"Authorization": f"Bearer {self.AZURE_MANAGEMENT_API_ACCESS_TOKEN}"}
        workspace_details_response = requests.get(workspace_url, headers=request_headers)
        workspace_details = workspace_details_response.json()
        error = workspace_details.get("error", None)
        assert "error" not in workspace_details, (
            f"Could not retrieve the Synapse workspace details for workspace '{workspace_name}'. The error from the REST API is: '{error}'"
        )
        return workspace_details

    def test_synapse_exists(self):
        workspace_details = self._get_synapse_workspace(self.WORKSPACE_NAME)
        assert workspace_details.get("name", None) == self.WORKSPACE_NAME

    def test_synapse_reachable(self):
        workspace_notebooks_url = f"{self.SYNAPSE_ENDPOINT}/notebooks?api-version=2020-12-01"
        headers = {"Authorization": f"Bearer {self.SYNAPSE_ACCESS_TOKEN}", "Content-Type": "application/json"}
        synapse_response = requests.get(workspace_notebooks_url, headers=headers)
        assert synapse_response.status_code == 200, (
            f"Expected the status code to be '200' when polling synapse, but was '{synapse_response.status_code}'. "
            "Check that the VPN is enabled on the test host, and that Synapse private endpoints exist and are enabled"
        )
    
    def test_purview_configuration(self):
        workspace_details = self._get_synapse_workspace(self.WORKSPACE_NAME)
        properties = workspace_details["properties"]
        purview_details = properties.get("purviewConfiguration", dict())
        expected_purview_id = TEST_CONFIG["PURVIEW_ID"]
        purview_resource_id = purview_details.get("purviewResourceId", None)
        assert purview_resource_id == expected_purview_id, f"Expected linked Purview account to be '{purview_resource_id}', but was '{expected_purview_id}'"
