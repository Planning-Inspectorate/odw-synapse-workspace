from typing import List, Set

from pipelines.scripts.packages.synapse_types import BigDataPoolResourceInfo, LibraryInfo

def _sorted_by_name(packages: List[LibraryInfo]) -> List[LibraryInfo]:
    return sorted(packages, key=lambda package: package["name"])

class SparkPoolConfiguration:
    """
    Class for interacting with Spark Pool configuration objects
    """
    def __init__(self, config: BigDataPoolResourceInfo) -> None:
        self.config = config
        self.packages = config["properties"].get("customLibraries", [])

    def get_updated_custom_libraries(self, local_packages: Set[str], odw_package_name: str, env: str) -> tuple[bool,List[LibraryInfo]]:
        """
        Returns whether the list of custom libraries should be updated, and
        a list of custom libraries to assign to the spark pool

        Keep the ODW package and any package that is defined locally
        """
        package_names = {package["name"] for package in self.packages}

        odw_packages = [
            package
            for package in self.packages
            if package["name"].startswith("odw")
        ]
        odw_package_names = {package["name"] for package in odw_packages}

        # if the new wheel isn't present, we need to update
        should_update_odw_packages = odw_package_name not in odw_package_names
        if not should_update_odw_packages:
            # if there are multiple odw packages, we need to update
            should_update_odw_packages = len(odw_package_names) > 1

        if should_update_odw_packages:
            odw_packages = [
                LibraryInfo(
                    name=odw_package_name,
                    path=f"pins-synw-odw-{env}-uks/libraries/{odw_package_name}",
                    containerName="prep",
                    uploadedTimestamp="0001-01-01T00:00:00+00:00",
                    type="whl"
                )
            ]

        # keep other packages if listed locally
        other_packages_to_keep = [
            package
            for package in self.packages
            if not package["name"].startswith("odw") and package["name"] in local_packages
        ]

        # add other packages from local
        other_packages_to_add = [
            LibraryInfo(
                name= package,
                path= f"pins-synw-odw-{env}-uks/libraries/{package}",
                containerName= "prep",
                uploadedTimestamp= "0001-01-01T00:00:00+00:00",
                type= "whl" if package.endswith("whl") else "jar"
            )
            for package in local_packages
            if package not in package_names
        ]

        updated_package_list = odw_packages + other_packages_to_keep + other_packages_to_add

        # Compare independently of ordering: a difference in order alone is not a change
        packages_changed = _sorted_by_name(updated_package_list) != _sorted_by_name(self.packages)
        return should_update_odw_packages or packages_changed, updated_package_list