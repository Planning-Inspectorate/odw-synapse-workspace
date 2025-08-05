from pipelines.scripts.util.exceptions import MaxWaitTimeNeededException
from pipelines.scripts.util.util import Util
from azure.identity import AzureCliCredential
import requests
import json
from typing import List, Dict, Any
import time


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

    def get_workspace_packages(self) -> List[Dict[str, Any]]:
        resp: requests.Response = requests.get(
            f"{self.ENDPOINT}/libraries?api-version=2021-06-01",
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        try:
            resp_json = resp.json()
            return resp_json["value"]
        except json.JSONDecodeError:
            pass
        raise ValueError(f"Http endpoint did not respond with a json object. Received {resp}")

    def upload_workspace_package(self, package_path: str):
        resp = json.loads(
            Util.run_az_cli_command(
                [
                    "az",
                    "synapse",
                    "workspace-package",
                    "upload",
                    "--workspace-name",
                    self.workspace_name,
                    "--package",
                    package_path,
                    "--no-progress"
                ]
            )
        )
        package_name = package_path.replace("dist/", "")
        max_wait_time = 5 * 60 # Wait 5 minutes
        current_wait_time = 0
        retry_delay_seconds = 20
        while current_wait_time < max_wait_time:
            workspace_package_names = {package["name"]: package for package in self.get_workspace_packages()}
            if package_name in workspace_package_names:
                if workspace_package_names[package_name]["properties"]["provisioningStatus"] == "Succeeded":
                    return resp
            current_wait_time += retry_delay_seconds
            time.sleep(retry_delay_seconds)
        raise MaxWaitTimeNeededException(f"Exceeded max wait time for creation of workspace package '{package_name}'")

    def remove_workspace_package(self, package_name: str):
        # This command returns nothing
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
        max_wait_time = 10 * 60 # Wait 10 minutes
        current_wait_time = 0
        retry_delay_seconds = 20
        while current_wait_time < max_wait_time:
            workspace_package_names = [package["name"] for package in self.get_workspace_packages()]
            if package_name not in workspace_package_names:
                return
            current_wait_time += retry_delay_seconds
            time.sleep(retry_delay_seconds)
        raise MaxWaitTimeNeededException(f"Exceeded max wait time for deletion of workspace package '{package_name}'")

    def get_spark_pool(self, spark_pool_name: str):
        resp = requests.Response = requests.get(
            f"{self.ENDPOINT}/bigDataPools/{spark_pool_name}?api-version=2021-06-01",
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        try:
            return resp.json()
        except json.JSONDecodeError:
            pass
        raise ValueError(f"http endpint did not respond with a json object. Received {resp}")

    def update_spark_pool(self, spark_pool_name: str, spark_pool_json: Dict[str, Any]):
        resp: requests.Response = requests.put(
            f"{self.ENDPOINT}/bigDataPools/{spark_pool_name}?api-version=2021-06-01",
            json=spark_pool_json,
            headers={"Authorization": f"Bearer {self._get_token()}"}
        )
        try:
            resp_json = resp.json()
        except json.JSONDecodeError:
            resp_json = None
        if not resp_json:
            raise ValueError(f"http endpint did not respond with a json object. Received {resp}")
        # Need to wait for the spark pool to exit provisioning state
        max_wait_time = 50 * 60 # Wait 50 minutes, this is a slow operation
        current_wait_time = 0
        retry_delay_seconds = 60
        while current_wait_time < max_wait_time:
            spark_pool = self.get_spark_pool(spark_pool_name)
            provisioning_state = spark_pool["properties"]["provisioningState"]
            if provisioning_state == "Succeeded":
                return resp_json
            if provisioning_state in {"Failed", "Canceled"}:
                raise ValueError(
                    f"Failed to provision the spark pool '{spark_pool_name}' - final state was '{provisioning_state}'. Please inspect the logs"
                )
            current_wait_time += retry_delay_seconds
            time.sleep(retry_delay_seconds)
        raise MaxWaitTimeNeededException(f"Exceeded max wait time for spark pool update for spark pool '{spark_pool_name}'")
