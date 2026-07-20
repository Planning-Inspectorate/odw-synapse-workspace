from pipelines.scripts.config import CONFIG
from pipelines.scripts.packages.gradle_build import extract_jar_files
from pipelines.scripts.packages.local_packages import get_local_workspace_packages
from pipelines.scripts.packages.spark_pool_configuration import SparkPoolConfiguration
from pipelines.scripts.packages.spark_pool_packages import get_workspace_packages
import argparse
from typing import Dict, Any, Set, Union, cast
from odw_common.util.synapse_workspace_manager import SynapseWorkspaceManager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import json

from pipelines.scripts.packages.synapse_types import BigDataPoolResourceInfo, LibraryRequirements

"""
Module for managing the packages deployed to a Synapse workspace, and binding to spark pools.
Includes packages defined in the `configuration/*` folders, and the main ODW package.

Example usage

`python3 pipelines/scripts/deploy_packages.py -e dev -d --odw_package_name odw-0.0.1.whl`  # Which would deploy to the dev environment
`pipelines/scripts/deploy_packages.py -e dev --odw_package_name odw-0.0.1.whl`  # Which would only validate the config, instead of deploying to Synapse
"""

# Configure the root logger to include a timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ODWPackageDeployer:
    """
    Class for managing the deployment of the packages to Synapse
    """

    # Map of spark pools to their associated requirements.txt files
    SPARK_POOL_REQUIREMENTS_MAP = {
        "pinssynspodwpr": "requirements-preview.txt",
        "pinssynspodw35": "requirements.txt",
    }
    # List of spark pools to update
    TARGET_SPARK_POOLS = SPARK_POOL_REQUIREMENTS_MAP.keys()

    def __init__(self, workspace_manager: SynapseWorkspaceManager, env: str, odw_package_name: str):
        self.workspace_manager = workspace_manager
        self.env = env
        self.odw_package_name = odw_package_name

    def get_odw_packages_bound_to_extra_spark_pools(self) -> Set[str]:
        """
            Return all odw packages in the workspace that are bound to spark pools not defined in TARGET_SPARK_POOLS
        """
        all_spark_pools = self.workspace_manager.get_all_spark_pools()
        extra_pools = [x for x in all_spark_pools if x["name"] not in self.TARGET_SPARK_POOLS]
        return {
            package["name"]
            for pool in extra_pools
            for package in pool["properties"].get("customLibraries", [])
            if package["name"].startswith("odw")
        }

    def generate_new_spark_pool_json(self, spark_pool_name: str) -> Union[Dict[str, Any], None]:
        """
        Generate new json for the spark pool (i.e. add the requirements and packages as defined in the configuration)

        :param spark_pool_name: The name of the spark pool to generate the json for
        :return: The enriched json with updated packages and requirements, or `None` if there are no changes
        """
        spark_pool = cast(BigDataPoolResourceInfo, self.workspace_manager.get_spark_pool(spark_pool_name))
        modified = False
        requirements_modified, new_requirements = self._get_requirements_modifications(spark_pool_name, spark_pool)
        if requirements_modified:
            modified = True
            spark_pool["properties"]["libraryRequirements"] = new_requirements

        config = SparkPoolConfiguration(spark_pool)
        libraries_modified, new_libraries = config.get_updated_custom_libraries(local_packages=get_local_workspace_packages(), env=self.env, odw_package_name=self.odw_package_name)
        if libraries_modified:
            modified = True
            spark_pool["properties"]["customLibraries"] = new_libraries
        if modified:
            return spark_pool
        return None

    def _get_requirements_modifications(self, spark_pool_name: str, spark_pool: Dict[str, Any]) -> tuple[bool, LibraryRequirements]:
        """
        Return True if the `libraryRequirements` property of the given spark pool is different to the local configuration,
        False otherwise. If True, also returns the new requirements.
        """
        if "properties" not in spark_pool:
            raise ValueError(f"'properties' attribute is expected on the spark pool with name '{spark_pool_name}', but was missing")

        if spark_pool_name not in self.SPARK_POOL_REQUIREMENTS_MAP.keys():
            # If there are no local requirements, but requirements are defined in the workspace, then there is a modification
            if "libraryRequirements" in spark_pool.get("properties"):
                return True, LibraryRequirements()
            # If there are no local requirements and no requirements in the workspace, then there is no modification
            return False, LibraryRequirements()

        requirements_file_name = self.SPARK_POOL_REQUIREMENTS_MAP[spark_pool_name]
        with open(f"configuration/spark-pool/{requirements_file_name}", "r") as f:
            requirements_file_content = f.read()
        library_requirements = cast(LibraryRequirements, spark_pool.get("properties").get("libraryRequirements", dict()))
        expected_library_requirements = {
            "filename": requirements_file_name,
            "content": requirements_file_content
        }
        library_requirements_cleaned = {
            "filename": library_requirements.get("filename", ""),
            "content": library_requirements.get("content", "").replace("\r", "")
        }
        new_requirements = LibraryRequirements(
            filename = requirements_file_name,
            content = requirements_file_content,
            time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        modified = expected_library_requirements != library_requirements_cleaned
        if not modified:
            return False, LibraryRequirements()
        return True, new_requirements

    def get_package_changes(self, extra_pool_packages: Set[str]) -> tuple[set[str], set[str], bool]:
        # do a gradle build first for Java dependencies
        extract_jar_files()

        # read packages from the workspace
        workspace_packages = get_workspace_packages(self.workspace_manager)
        # read packages from the local repo
        local_packages = get_local_workspace_packages()

        # local packages not in the workspace
        packages_to_add = local_packages.difference(workspace_packages.package_names)
        # odw packages not in use by other pools and not the current package name
        odw_packages_to_remove = workspace_packages.odw_names.difference(extra_pool_packages).difference({self.odw_package_name})
        # workspace packages not local + any odw packages to remove
        # (this may remove non-odw packages from other spark pools, they should be added to requirements)
        packages_to_remove = workspace_packages.package_names.difference(local_packages).union(odw_packages_to_remove)
        # check if the odw package is in the list
        add_odw_package = self.odw_package_name not in workspace_packages.odw_names
        return packages_to_add, packages_to_remove, add_odw_package

    def update_synapse_packages(self, deploy: bool):
        """
        Compares the local configuration and the workspace, and decides which actions to take
        Will add/remove packages from the workspace, and from individual spark pools
        """

        odw_package_assignments_to_extra_pools = self.get_odw_packages_bound_to_extra_spark_pools()

        packages_to_add, packages_to_remove, add_odw_package = self.get_package_changes(odw_package_assignments_to_extra_pools)
        logging.info(f"packages to add to the workspace: {json.dumps(list(packages_to_add), indent=4)}")
        logging.info(f"packages to remove from the workspace: {json.dumps(list(packages_to_remove), indent=4)}")
        logging.info(f"odw package {self.odw_package_name} {'to add to the workspace' if add_odw_package else 'is present in the workspace'}")

        spark_pool_updates = dict()
        for spark_pool_name in self.TARGET_SPARK_POOLS:
            new_spark_pool_json = self.generate_new_spark_pool_json(spark_pool_name)
            if new_spark_pool_json:
                spark_pool_updates[spark_pool_name] = new_spark_pool_json

        if deploy:
            logging.info("deploying packages")
            for package in packages_to_add:
                logging.info(f"Uploading package '{package}' to the workspace")
                self.workspace_manager.upload_workspace_package(f"configuration/workspace-packages/{package}")
            if add_odw_package:
                logging.info(f"Uploading odw package '{self.odw_package_name}' to the workspace")
                self.workspace_manager.upload_workspace_package(f"dist/{self.odw_package_name}")
            # Update spark pools. Note this is a very slow operation, so it is done in parallel for all pools
            with ThreadPoolExecutor() as tpe:
                # Update all relevant spark pools in parallel to boost performance
                # Consume the iterator to ensure all updates complete
                list(
                    tpe.map(
                        self.workspace_manager.update_spark_pool,
                        spark_pool_updates.keys(),
                        spark_pool_updates.values()
                    )
                )
            logging.info(f"removing {len(packages_to_remove)} packages")
            for package in packages_to_remove:
                logging.info(f"removing {package} from the workspace")
                self.workspace_manager.remove_workspace_package(package)
            logging.info("deployment complete")
        else:
            logging.info("dry run, not deploying changes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", required=True, help="The environment to target")
    parser.add_argument("-d", "--deploy", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("-pn", "--odw_package_name", required=True, help="The name of the new odw wheel to deploy")
    args = parser.parse_args()
    synapse_workspace_manager = SynapseWorkspaceManager(
        f"pins-synw-odw-{args.env}-uks",
        CONFIG["SUBSCRIPTION_ID"],
        f"pins-rg-data-odw-{args.env}-uks"
    )
    package_deployer = ODWPackageDeployer(synapse_workspace_manager, args.env, args.odw_package_name)
    package_deployer.update_synapse_packages(args.deploy)
