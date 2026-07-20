import mock

from pipelines.scripts.packages.spark_pool_packages import (
    WorkspacePackages,
    get_workspace_packages
)
from odw_common.util.synapse_workspace_manager import SynapseWorkspaceManager

WORKSPACE_NAME = "synw"
SUBSCRIPTION_ID = "sub-1"
RESOURCE_GROUP = "rg-1"


def test_partitions_odw_and_non_odw_packages():
    packages = [
        {"name": "odw-1.0.0-abc-py3-none-any.whl"},
        {"name": "mypackage.whl"},
        {"name": "odw-1.0.0-def-py3-none-any.whl"},
        {"name": "otherlib.jar"},
    ]
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=packages):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert isinstance(result, WorkspacePackages)
    assert result.package_names == {"mypackage.whl", "otherlib.jar"}
    assert result.odw_names == {
        "odw-1.0.0-abc-py3-none-any.whl",
        "odw-1.0.0-def-py3-none-any.whl",
    }
    assert [package["name"] for package in result.packages] == ["mypackage.whl", "otherlib.jar"]
    assert [package["name"] for package in result.odw] == [
        "odw-1.0.0-abc-py3-none-any.whl",
        "odw-1.0.0-def-py3-none-any.whl",
    ]


def test_empty_workspace_returns_empty_collections():
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=[]):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert result.packages == []
    assert result.odw == []
    assert result.package_names == set()
    assert result.odw_names == set()


def test_all_odw_packages():
    packages = [
        {"name": "odw-1.0.0-abc-py3-none-any.whl"},
        {"name": "odw-1.0.0-def-py3-none-any.whl"},
    ]
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=packages):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert result.packages == []
    assert result.package_names == set()
    assert result.odw_names == {
        "odw-1.0.0-abc-py3-none-any.whl",
        "odw-1.0.0-def-py3-none-any.whl",
    }


def test_no_odw_packages():
    packages = [
        {"name": "mypackage.whl"},
        {"name": "otherlib.jar"},
    ]
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=packages):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert result.odw == []
    assert result.odw_names == set()
    assert result.package_names == {"mypackage.whl", "otherlib.jar"}


def test_only_names_with_odw_prefix_are_treated_as_odw():
    packages = [
        {"name": "not-odw-package.whl"},
        {"name": "odwsomething.whl"},
    ]
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=packages):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert result.odw_names == {"odwsomething.whl"}
    assert result.package_names == {"not-odw-package.whl"}


def test_duplicate_names_are_deduplicated_in_name_sets():
    packages = [
        {"name": "mypackage.whl"},
        {"name": "mypackage.whl"},
        {"name": "odw-1.0.0-abc-py3-none-any.whl"},
        {"name": "odw-1.0.0-abc-py3-none-any.whl"},
    ]
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=packages):
        result = get_workspace_packages(SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP))

    assert len(result.packages) == 2
    assert result.package_names == {"mypackage.whl"}
    assert len(result.odw) == 2
    assert result.odw_names == {"odw-1.0.0-abc-py3-none-any.whl"}


def test_get_workspace_packages_called_on_manager():
    with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=[]):
        manager = SynapseWorkspaceManager(WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP)
        get_workspace_packages(manager)

        manager.get_workspace_packages.assert_called_once_with()
