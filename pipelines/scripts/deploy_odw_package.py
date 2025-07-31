from pipelines.scripts.config import CONFIG
from pipelines.scripts.util import Util
from azure.identity import AzureCliCredential
import requests
import argparse
import json
from typing import List, Dict, Any
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO)


TARGET_SPARK_POOLS = [
    "pinssynspodwpr",
    "pinssynspodw34"
]


class SynapseWorkspaceManager():
    """
        Class to interact with Synapse Private Link private endpoints

        Note: API/CLI support for this kind of endpoint is quite limited. Only getting endpoints is available
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


def get_existing_odw_wheels(workspace_manager: SynapseWorkspaceManager) -> List[Dict[str, Any]]:
    packages = workspace_manager.get_workspace_packages()
    odw_packages = [package for package in packages if package["name"].startswith("odw")]
    return sorted(
        odw_packages,
        key=lambda package: datetime.strptime(package["properties"]["uploadedTimestamp"].replace("+00:00", "")[:-8], "%Y-%m-%dT%H:%M:%S")
    )


#def download_wheel(workspace_name: str, wheel_name: str) -> bool:
#    Util.run_az_cli_command()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", required=True, help="The environment to target")
    parser.add_argument("-wn", "--new_wheel_name", required=True, help="The name of the new odw wheel to deploy")
    args = parser.parse_args()
    env = args.env
    new_wheel_name = args.new_wheel_name
    workspace_name = f"pins-synw-odw-{env}-uks"
    subscription = CONFIG["SUBSCRIPTION_ID"]
    resource_group = f"pins-rg-data-odw-{env}-uks"
    synapse_workspace_manager = SynapseWorkspaceManager(workspace_name, subscription, resource_group)
    # Get existing workspace packages
    existing_wheels = get_existing_odw_wheels(synapse_workspace_manager)
    existing_wheel_names = {x["name"] for x in existing_wheels}
    if new_wheel_name in existing_wheel_names:
        raise ValueError(f"Cannot upload the wheel '{new_wheel_name}' because it already exists in the workspace, aborting deployment")
    most_recent_wheel = existing_wheels[-1] if existing_wheels else None
    logging.info("Uploading new workspace package")
    synapse_workspace_manager.upload_workspace_package(f"dist/{new_wheel_name}")
    spark_pools = {spark_pool: synapse_workspace_manager.get_spark_pool(spark_pool) for spark_pool in TARGET_SPARK_POOLS}
    existing_spark_pool_packages = {
        spark_pool: spark_pool_json["properties"]["customLibraries"] if "customLibraries" in spark_pool_json["properties"] else []
        for spark_pool, spark_pool_json in spark_pools.items()
    }
    new_pool_packages = {
        spark_pool: [
            package
            for package in spark_pool_packages
            if not package["name"].startswith("odw")
        ] + [
            {
                "name": new_wheel_name,
                "path": f"pins-synw-odw-dev-uks/libraries/{new_wheel_name}",
                "containerName": "prep",
                "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                "type": "whl"
            }
        ]
        for spark_pool, spark_pool_packages in existing_spark_pool_packages.items()
    }
    pool_json_map = {
        spark_pool: {
            k: v if k != "properties" else v | {"customLibraries": new_pool_packages[spark_pool]}
            for k, v in spark_pool_json.items()
        }
        for spark_pool, spark_pool_json in spark_pools.items()
    }
    logging.info("Updating spark pool packages")
    resp = synapse_workspace_manager.update_sparkpool("pinssynspodw34", pool_json_map["pinssynspodw34"])
    logging.info(json.dumps(resp, indent=4))
    logging.info("Removing old workspace packages")
    for package in existing_wheel_names:
        synapse_workspace_manager.remove_workspace_package(package)
