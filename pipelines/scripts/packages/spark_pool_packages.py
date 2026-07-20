from odw_common.util.synapse_workspace_manager import SynapseWorkspaceManager
from typing import List, TypedDict, Set, cast

from pipelines.scripts.packages.synapse_types import LibraryInfo

class LibrariesResponseItem(TypedDict, total=False):
    """
    https://learn.microsoft.com/en-us/rest/api/synapse/resourcemanager/libraries/list-by-workspace?view=rest-synapse-resourcemanager-2021-06-01&tabs=HTTP#libraryresource
    """
    id: str
    name: str
    type: str
    properties: LibraryInfo

class WorkspacePackages:
    """
    Package information for a Synapse workspace
    """
    packages: List[LibrariesResponseItem]
    package_names: Set[str]
    odw: List[LibrariesResponseItem]
    odw_names: Set[str]

def get_workspace_packages(workspace_manager: SynapseWorkspaceManager) -> WorkspacePackages:
    packages = cast(List[LibrariesResponseItem],  workspace_manager.get_workspace_packages())

    ws = WorkspacePackages()
    ws.packages = [package for package in packages if not package["name"].startswith("odw")]
    ws.package_names = set(x["name"] for x in ws.packages)
    ws.odw = [package for package in packages if package["name"].startswith("odw")]
    ws.odw_names = set(x["name"] for x in ws.odw)
    return ws