from pipelines.scripts.util.util import Util
from azure.identity import AzureCliCredential
import requests
import json
from typing import Dict, Any


class SynapseWorkspaceManager():
    """
        Class to interact with a Synapse workspace
    """
    _token = None

    def __init__(self, workspace_name: str, subscription_id: str, resource_group_name: str):
        self.workspace_name = workspace_name
        self.ENDPOINT = (
            f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/"
            f"Microsoft.Synapse/workspaces/{workspace_name}"
        )

    @classmethod
    def _get_token(cls) -> str:
        if not (cls._token):
            cls._token = AzureCliCredential().get_token("https://management.azure.com/.default").token
        return cls._token

    def get_workspace_packages(self) -> Dict[str, Any]:
        resp: requests.Response = requests.get(
            f"{self.ENDPOINT}/libraries?api-version=2021-06-01",
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        if "application/json" in resp.headers.get("Content-Type", ""):
            resp_json = resp.json()
            if "value" in resp_json:
                return resp_json["value"]
            raise ValueError(f"response raised an exception: {json.dumps(resp_json, indent=4)}")
        raise ValueError(f"http endpoint did not respond with a json object. Received {resp}")
    
    def upload_workspace_package(self, package_name: str):
        return json.loads(
            Util.run_az_cli_command(
                [
                    "az",
                    "synapse",
                    "workspace-package",
                    "upload",
                    "--workspace-name",
                    self.workspace_name,
                    "--package",
                    package_name,
                    "--no-progress"
                ]
            )
        )
    
    def remove_workspace_package(self, package_name: str):
        resp = json.loads(
            Util.run_az_cli_command(
                [
                    "az",
                    "synapse",
                    "workspace-package",
                    "delete",
                    "--workspace-name",
                    self.workspace_name,
                    "--package",
                    package_name,
                    "--no-wait",
                    "-y"
                ]
            )
        )
        # Need to wait for the package to be deleted before continuing
        return resp
    
    def get_spark_pool(self, spark_pool_name: str):
        resp = requests.Response = requests.get(
            f"{self.ENDPOINT}/bigDataPools/{spark_pool_name}?api-version=2021-06-01",
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        if "application/json" in resp.headers.get("Content-Type", ""):
            return resp.json()
        raise ValueError(f"http endpint did not respond with a json object. Received {resp}")

    def update_sparkpool(self, spark_pool_name: str, spark_pool_json: Dict[str, Any]):
        resp: requests.Response = requests.put(
            f"{self.ENDPOINT}/bigDataPools/{spark_pool_name}?api-version=2021-06-01",
            json=spark_pool_json,
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        if "application/json" in resp.headers.get("Content-Type", ""):
            # Need to wait for the spark pool to exit provisioning state
            return resp.json()
        raise ValueError(f"http endpint did not respond with a json object. Received {resp}")
