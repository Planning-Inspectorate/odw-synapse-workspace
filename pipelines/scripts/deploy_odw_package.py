from odw_common.util.synapse_workspace_manager import SynapseWorkspaceManager
from concurrent.futures import ThreadPoolExecutor
from pipelines.scripts.config import CONFIG
import argparse
from typing import List, Dict, Any
from datetime import datetime
import logging
import json


logging.basicConfig(level=logging.INFO)


class ODWPackageDeployer():
    """
        Class for managing the deployment of the ODW Python Package
    """
    TARGET_SPARK_POOLS = [
        "pinssynspodw34",
        "pinssynspodwpr"
    ]

    def get_existing_odw_wheels(self, workspace_manager: SynapseWorkspaceManager) -> List[Dict[str, Any]]:
        """
            Return all ODW Python wheels currently deployed in the workspace
        """
        packages = workspace_manager.get_workspace_packages()
        odw_packages = [package for package in packages if package["name"].startswith("odw")]
        return sorted(
            odw_packages,
            key=lambda package: datetime.strptime(package["properties"]["uploadedTimestamp"].replace("+00:00", "")[:-8], "%Y-%m-%dT%H:%M:%S")
        )

    def get_odw_packages_bound_to_extra_spark_pools(self, workspace_manager: SynapseWorkspaceManager):
        """
            Return all odw packages in the workspace that are bound to spark pools not defined in TARGET_SPARK_POOLS
        """
        all_spark_pools = workspace_manager.get_all_spark_pools()
        extra_pools = [x for x in all_spark_pools if x["name"] not in self.TARGET_SPARK_POOLS]
        return {
            package["name"]
            for pool in extra_pools
            for package in pool["properties"].get("customLibraries", [])
            if "odw" in package["name"]
        }

    def upload_new_wheel(self, env: str, new_wheel_name: str):
        """
            Upload the new wheel to the target environment

            This follows the below process
            1. If the current odw package version (for the current branch) does not exist in the workspace, then upload it
            2. Bind the package to each pool in `self.TARGET_SPARK_POOLS` if it is not already bound, and unbind all other odw packages from these pools
            3. Remove all odw packages from the workspace that are not bound to any pool (including pools outside of `TARGET_SPARK_POOLS`)
        """
        workspace_name = f"pins-synw-odw-{env}-uks"
        subscription = CONFIG["SUBSCRIPTION_ID"]
        resource_group = f"pins-rg-data-odw-{env}-uks"
        synapse_workspace_manager = SynapseWorkspaceManager(workspace_name, subscription, resource_group)
        # Get existing workspace packages
        existing_wheels = self.get_existing_odw_wheels(synapse_workspace_manager)
        existing_wheel_names = {x["name"] for x in existing_wheels}
        need_to_upload_to_workspace = new_wheel_name not in existing_wheel_names
        if need_to_upload_to_workspace:
            logging.info("Uploading new workspace package")
            synapse_workspace_manager.upload_workspace_package(f"dist/{new_wheel_name}")
        else:
            logging.info(f"The new wheel '{new_wheel_name}' already exists in the workspace - skipping uploading to the workspace")
        # Prepare to update the spark pools to use the new package
        initial_spark_pool_json_map = {spark_pool: synapse_workspace_manager.get_spark_pool(spark_pool) for spark_pool in self.TARGET_SPARK_POOLS}
        # Only apply the update to pools that need to be updated
        initial_spark_pool_json_map = {
            k: v
            for k, v in initial_spark_pool_json_map.items()
            if new_wheel_name not in {x["name"] for x in v["properties"].get("customLibraries", [])} or len(
                [x["name"] for x in v["properties"].get("customLibraries", []) if x["name"].startswith("odw")]
            ) > 1
        }
        existing_spark_pool_packages = {
            spark_pool: spark_pool_json["properties"]["customLibraries"] if "customLibraries" in spark_pool_json["properties"] else []
            for spark_pool, spark_pool_json in initial_spark_pool_json_map.items()
        }
        # Enrich the customLibraries by removing the old odw packages and adding the new one
        new_pool_packages = {
            spark_pool: [
                package
                for package in spark_pool_packages
                if not package["name"].startswith("odw")
            ] + [
                {
                    "name": new_wheel_name,
                    "path": f"pins-synw-odw-{env}-uks/libraries/{new_wheel_name}",
                    "containerName": "prep",
                    "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                    "type": "whl"
                }
            ]
            for spark_pool, spark_pool_packages in existing_spark_pool_packages.items()
        }
        # Update the base spark pool json with the new customLibraries
        new_spark_pool_json_map = {
            spark_pool: {
                k: v if k != "properties" else v | {"customLibraries": new_pool_packages[spark_pool]}  # Overwrite the properties.custonLibraries attribute
                for k, v in spark_pool_json.items()
            }
            for spark_pool, spark_pool_json in initial_spark_pool_json_map.items()
        }
        if new_spark_pool_json_map:
            logging.info("Updating spark pool packages (This is a slow operation, and can take between 20 and 50 minutes)")
            logging.info(f"The below spark pools will be updated:\n{json.dumps(new_spark_pool_json_map, indent=4)}")
            spark_pool_names_to_update = list(new_spark_pool_json_map.keys())
            with ThreadPoolExecutor() as tpe:
                # Update all relevant spark pools in parallel to boost performance
                [
                    thread_response
                    for thread_response in tpe.map(
                        synapse_workspace_manager.update_spark_pool,
                        spark_pool_names_to_update,
                        [new_spark_pool_json_map[pool] for pool in spark_pool_names_to_update]
                    )
                    if thread_response
                ]
        else:
            logging.info(F"The spark pools {self.TARGET_SPARK_POOLS} already have the new odw package bound to them")
        logging.info("Removing odw packages that are not assigned to any pool")
        odw_package_assignments_to_extra_pools = self.get_odw_packages_bound_to_extra_spark_pools(synapse_workspace_manager)
        odw_packages_to_delete = existing_wheel_names.difference(odw_package_assignments_to_extra_pools).difference({new_wheel_name})
        logging.info(
            f"The following packages will be removed: {json.dumps(list(odw_packages_to_delete), indent=4)}"
        )
        for package in odw_packages_to_delete:
            synapse_workspace_manager.remove_workspace_package(package)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", required=True, help="The environment to target")
    parser.add_argument("-wn", "--new_wheel_name", required=True, help="The name of the new odw wheel to deploy")
    args = parser.parse_args()
    env = args.env
    new_wheel_name = args.new_wheel_name
    ODWPackageDeployer().upload_new_wheel(env, new_wheel_name)
