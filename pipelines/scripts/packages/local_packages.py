from typing import Set
import os

def get_local_workspace_packages() -> Set[str]:
    """
    Return the names of the packages that are defined in the local configuration
    """
    package_path = "configuration/workspace-packages"
    if not os.path.exists(package_path):
        return set()
    allowed_extensions = [".whl", ".jar"]
    return set(
        x
        for x in os.listdir(package_path)
        if os.path.splitext(x)[1] in allowed_extensions
    )