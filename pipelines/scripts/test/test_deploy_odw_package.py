from odw_common.util.synapse_workspace_manager import SynapseWorkspaceManager
from pipelines.scripts.deploy_odw_package import ODWPackageDeployer
import pytest
import mock


def test_get_odw_wheels():
    mock_get_workspace_packages = [
        {
            "name": "packageA.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-05T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_other_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-05T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_other_other_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-02T13:58:54.3370407+00:00"
            }
        }
    ]
    expected_odw_packages = [
        {
            "name": "odw-1.0.0-some_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_other_other_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-02T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_other_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-05T13:58:54.3370407+00:00"
            }
        }
    ]
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager") as mock_workspace_manager:
        mock_workspace_manager.get_workspace_packages.return_value = mock_get_workspace_packages
        actual_odw_packages = ODWPackageDeployer().get_existing_odw_wheels(mock_workspace_manager)
        assert actual_odw_packages == expected_odw_packages


def test_get_odw_packages_bound_to_extra_spark_pools():
    """
        Given i have some spark pools with odw packages bound to them
        When i call get_odw_packages_bound_to_extra_spark_pools
        Then only the odw packages not bound to the main pools should be returned
    """
    mock_spark_pools = [
        {
            "name": "someOtherPoolA",
            "properties": {
                "customLibraries": [
                    {
                        "name": "odwPackageA"
                    },
                    {
                        "name": "odwPackageB"
                    }
                ]
            }
        },
        {
            "name": "someOtherPoolB",
            "properties": {
                "customLibraries": [
                    {
                        "name": "odwPackageC"
                    }
                ]
            }
        },
        {
            "name": "someOtherPoolC",
            "properties": {
                "customLibraries": [
                    {
                        "name": "odwPackageD"
                    }
                ]
            }
        },
        {
            "name": "mockMainPool",
            "properties": {
                "customLibraries": [
                    {
                        "name": "odwPackageD"
                    },
                    {
                        "name": "odwPackageE"  # This pool should not be returned
                    }
                ]
            }
        }
    ]
    expected_odw_packages_bound_to_other_pools = {
        "odwPackageA",
        "odwPackageB",
        "odwPackageC",
        "odwPackageD"
    }
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager") as mock_workspace_manager:
        with mock.patch.object(mock_workspace_manager, "get_all_spark_pools", return_value=mock_spark_pools):
            with mock.patch.object(ODWPackageDeployer, "TARGET_SPARK_POOLS", ["mockMainPool"]):
                assert ODWPackageDeployer().get_odw_packages_bound_to_extra_spark_pools(
                    mock_workspace_manager
                ) == expected_odw_packages_bound_to_other_pools


def test_upload_new_wheel__with_no_existing_package():
    """
        Given there are no odw packages in the workspace
        When i upload a package for the first time
        Then the package should be uploaded to the workspace, and bound to the spark pools
    """
    env = "mock_env"
    wheel_name = "odw_test_wheel.whl"
    def get_spark_pool(inst, pool_name: str):
        if pool_name == "pinssynspodwpr":
            return {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "odw-old-wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw-old-wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0005-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        if pool_name == "pinssynspodw34":
            return {
                "properties": dict()
            }
        pytest.fail(f"Unexpected pool name '{pool_name}' used in the test - please review this")
    mock_odw_wheels = []  # No other odw wheels
    expected_wheel_upload_calls = [
        mock.call(
            "pinssynspodwpr",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "odw_test_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_test_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        ),
        mock.call(
            "pinssynspodw34",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "odw_test_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_test_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        )
    ]
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", get_spark_pool):
                with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
                            with mock.patch.object(
                                ODWPackageDeployer,
                                "get_odw_packages_bound_to_extra_spark_pools",
                                return_value=set()
                            ):
                                ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                SynapseWorkspaceManager.upload_workspace_package.assert_called_once_with("dist/odw_test_wheel.whl")
                                SynapseWorkspaceManager.update_spark_pool.assert_has_calls(expected_wheel_upload_calls, any_order=True)
                                assert not SynapseWorkspaceManager.remove_workspace_package.called



def test_upload_new_wheel__with_other_odw_package():
    """
        Given a different odw package exists in the workspace, and the new package does not exist in the workspace yet
        When i upload a new package
        Then the old package should be removed from the workspace and pools, and replaced by the new package
    """
    env = "mock_env"
    wheel_name = "odw_test_wheel.whl"
    def get_spark_pool(inst, pool_name: str):
        if pool_name == "pinssynspodwpr":
            return {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "odw-old-wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw-old-wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0005-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        if pool_name == "pinssynspodw34":
            return {
                "properties": dict()
            }
        pytest.fail(f"Unexpected pool name '{pool_name}' used in the test - please review this")
    mock_odw_wheels = [
        {
            "name": "odw-1.0.0-some_commit-py3-none-any.whl",  # This wheel is expected to be deleted from the workspace
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        }
    ]
    expected_wheel_upload_calls = [
        mock.call(
            "pinssynspodwpr",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "odw_test_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_test_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        ),
        mock.call(
            "pinssynspodw34",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "odw_test_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_test_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        )
    ]
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", get_spark_pool):
                with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
                            with mock.patch.object(
                                ODWPackageDeployer,
                                "get_odw_packages_bound_to_extra_spark_pools",
                                return_value=set()
                            ):
                                ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                SynapseWorkspaceManager.upload_workspace_package.assert_called_once_with(
                                    "dist/odw_test_wheel.whl"
                                )
                                SynapseWorkspaceManager.update_spark_pool.assert_has_calls(
                                    expected_wheel_upload_calls,
                                    any_order=True
                                )
                                SynapseWorkspaceManager.remove_workspace_package.assert_called_once_with(
                                    "odw-1.0.0-some_commit-py3-none-any.whl"
                                )


def test_upload_new_wheel__with_duplicate_existing_package():
    """
        Given the new odw package already exists in the workspace
        When i try to upload a new package
        The deployment should only skip uploading the package to the workspace
    """
    env = "mock_env"
    wheel_name = "odw_test_wheel.whl"
    mock_odw_wheels = [
        {
            "name": "odw_test_wheel.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        }
    ]
    mock_spark_pool = {"name": "some_pool", "properties": {}}
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
            with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
                with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool):
                        with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                            with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                                with mock.patch.object(
                                    ODWPackageDeployer,
                                    "get_odw_packages_bound_to_extra_spark_pools",
                                    return_value=set()
                                ):
                                    ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                    assert not SynapseWorkspaceManager.upload_workspace_package.called


def test_upload_new_wheel__with_package_already_bound_to_spark_pool():
    """
        Given the new odw package already exists in the workspace, and is bound to the [pinssynspodwpr or pinssynspodw34] spark pools
        When i try to upload a new package
        The deployment should skip updating the spark pools
    """
    
    env = "mock_env"
    wheel_name = "odw_test_wheel.whl"
    mock_odw_wheels = [
        {
            "name": "odw_test_wheel.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        }
    ]
    mock_spark_pool = {
        "properties": {
            "customLibraries": [
                {
                    "name": wheel_name,
                    "path": f"pins-synw-odw-{env}-uks/libraries/{wheel_name}",
                    "containerName": "prep",
                    "uploadedTimestamp": "0005-01-01T00:00:00+00:00",
                    "type": "whl"
                }
            ]
        }
    }
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
            with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
                with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool):
                        with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                            with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                                with mock.patch.object(
                                    ODWPackageDeployer,
                                    "get_odw_packages_bound_to_extra_spark_pools",
                                    return_value=set()
                                ):
                                    ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                    assert SynapseWorkspaceManager.get_spark_pool.called
                                    assert not SynapseWorkspaceManager.update_spark_pool.called


def test_upload_new_wheel__with_existing_odw_package_already_bound_to_external_spark_pool():
    """
        Given there is already an odw package in the workspace bound to a pool other than [pinssynspodwpr or pinssynspodw34]
        When i try to upload a new odw package
        Then the [pinssynspodwpr or pinssynspodw34] pools should be updated to use the new package, but the old package must be left as-is
    """
    env = "mock_env"
    wheel_name = "odw_new_wheel.whl"
    def get_spark_pool(inst, pool_name: str):
        if pool_name == "pinssynspodwpr":
            return {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "odw_wheel_bound_to_other_pool.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_wheel_bound_to_other_pool.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0005-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        if pool_name == "pinssynspodw34":
            return {
                "properties": dict()
            }
        if pool_name == "some_external_spark_pool":
            return {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "odw_wheel_bound_to_other_pool.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/odw_wheel_bound_to_other_pool.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0005-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        pytest.fail(f"Unexpected pool name '{pool_name}' used in the test - please review this")
    mock_odw_wheels = [
        {
            "name": "odw_wheel_bound_to_other_pool.whl",  # This wheel is expected to be kept in the workspace
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        }
    ]
    expected_wheel_upload_calls = [
        mock.call(
            "pinssynspodwpr",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": "some_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": "some_other_wheel.whl",
                            "path": f"pins-synw-odw-{env}-uks/libraries/some_other_wheel.whl",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        },
                        {
                            "name": wheel_name,
                            "path": f"pins-synw-odw-{env}-uks/libraries/{wheel_name}",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        ),
        mock.call(
            "pinssynspodw34",
            {
                "properties": {
                    "customLibraries": [
                        {
                            "name": wheel_name,
                            "path": f"pins-synw-odw-{env}-uks/libraries/{wheel_name}",
                            "containerName": "prep",
                            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
                            "type": "whl"
                        }
                    ]
                }
            }
        )
    ]
    with mock.patch("odw_common.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", get_spark_pool):
                with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
                            with mock.patch.object(
                                ODWPackageDeployer,
                                "get_odw_packages_bound_to_extra_spark_pools",
                                return_value={"odw_wheel_bound_to_other_pool.whl"}
                            ):
                                ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                SynapseWorkspaceManager.upload_workspace_package.assert_called_once_with(f"dist/{wheel_name}")
                                SynapseWorkspaceManager.update_spark_pool.assert_has_calls(
                                    expected_wheel_upload_calls,
                                    any_order=True
                                )
                                assert not SynapseWorkspaceManager.remove_workspace_package.called
