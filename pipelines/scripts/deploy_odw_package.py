from pipelines.scripts.util.synapse_workspace_manager import SynapseWorkspaceManager
from pipelines.scripts.util.exceptions import ConcurrentWheelUploadException
from concurrent.futures import ThreadPoolExecutor
from pipelines.scripts.config import CONFIG
import argparse
from typing import List, Dict, Any
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO)


class ODWPackageDeployer():
    """
        Class for managing the deployment of the ODW Python Package
    """
    TARGET_SPARK_POOLS = [
        "pinssynspodwpr",
        "pinssynspodw34"
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

    def upload_new_wheel(self, env: str, new_wheel_name: str):
        """
            Upload the new wheel to the target environment

            This follows the below process
            1. If there are already 2 or more ODW packages in the workspace, then abort with an exception (there is likely to be another deployment ongoing)
            2. If the new wheel already exists in the workspace, abort (it has already been uploaded)
            3. Upload the new wheel to the Synapse workspace
            4. Update the two spark pools to use the new ODW package (and to un-link the old ODW package)
            5. Remove the old ODW packages from the workspace
        """
        workspace_name = f"pins-synw-odw-{env}-uks"
        subscription = CONFIG["SUBSCRIPTION_ID"]
        resource_group = f"pins-rg-data-odw-{env}-uks"
        synapse_workspace_manager = SynapseWorkspaceManager(workspace_name, subscription, resource_group)
        # Get existing workspace packages
        existing_wheels = self.get_existing_odw_wheels(synapse_workspace_manager)
        existing_wheel_names = {x["name"] for x in existing_wheels}
        if len(existing_wheels) > 1:
            raise ConcurrentWheelUploadException(
                (
                    f"There are {len(existing_wheels)} odw wheels already deployed in workspace '{workspace_name}', "
                    "which indicates another wheel deployment is in progress. Please wait for this concurrent run to complete. "
                    "If there are no other concurrent runs, then please review the deployed wheels, and remove whichever wheel is not applied "
                    "manually"
                )
            )
        if new_wheel_name in existing_wheel_names:
            # The assumption is that each wheel name contains the commit hash of the newest commit. This hash identifies the wheel version
            # If the new wheel name already exists in synapse, then there is no need to upload again
            logging.info(f"Cannot upload the wheel '{new_wheel_name}' because it already exists in the workspace, aborting deployment")
            return
        logging.info("Uploading new workspace package")
        synapse_workspace_manager.upload_workspace_package(f"dist/{new_wheel_name}")
        # Prepare to update the spark pools to use the new package
        initial_spark_pool_json_map = {spark_pool: synapse_workspace_manager.get_spark_pool(spark_pool) for spark_pool in self.TARGET_SPARK_POOLS}
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
        logging.info("Updating spark pool packages (This is a slow operation, and can take between 20 and 50 minutes)")
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
        logging.info("Removing old workspace packages")
        for package in existing_wheel_names:
            synapse_workspace_manager.remove_workspace_package(package)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", required=True, help="The environment to target")
    parser.add_argument("-wn", "--new_wheel_name", required=True, help="The name of the new odw wheel to deploy")
    args = parser.parse_args()
    env = args.env
    new_wheel_name = args.new_wheel_name
    ODWPackageDeployer().upload_new_wheel(env, new_wheel_name)
