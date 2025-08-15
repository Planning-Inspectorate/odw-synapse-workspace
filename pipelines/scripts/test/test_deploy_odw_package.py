from pipelines.scripts.util.synapse_workspace_manager import SynapseWorkspaceManager
from pipelines.scripts.deploy_odw_package import ODWPackageDeployer
from pipelines.scripts.util.exceptions import ConcurrentWheelUploadException
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
    with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager") as mock_workspace_manager:
        mock_workspace_manager.get_workspace_packages.return_value = mock_get_workspace_packages
        actual_odw_packages = ODWPackageDeployer().get_existing_odw_wheels(mock_workspace_manager)
        assert actual_odw_packages == expected_odw_packages


def test_upload_new_wheel__with_no_existing_package():
    """
        Test uploading an odw package for the first time
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
    with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", get_spark_pool):
                with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value = mock_odw_wheels):
                            ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                            SynapseWorkspaceManager.upload_workspace_package.assert_called_once_with(f"dist/odw_test_wheel.whl")
                            SynapseWorkspaceManager.update_spark_pool.assert_has_calls(expected_wheel_upload_calls, any_order=True)
                            assert not SynapseWorkspaceManager.remove_workspace_package.called



def test_upload_new_wheel__with_other_odw_package():
    """
        Test uploading an odw package to a workspace that has an "older" version of the package
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
    with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", get_spark_pool):
                with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value = mock_odw_wheels):
                            ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                            SynapseWorkspaceManager.upload_workspace_package.assert_called_once_with(f"dist/odw_test_wheel.whl")
                            SynapseWorkspaceManager.update_spark_pool.assert_has_calls(expected_wheel_upload_calls, any_order=True)
                            SynapseWorkspaceManager.remove_workspace_package.assert_called_once_with("odw-1.0.0-some_commit-py3-none-any.whl")


def test_upload_new_wheel__with_two_other_existing_packages():
    """
        Test that trying to upload the odw package when there are already two of them raises an exception

        i.e. The assumption is that the initial odw package rollout is carefully done, which ensures there is always at least 1 package
             installed. Subsequent deployments add a package and remove it afterwards. If there are already two odw packages installed, then
             it means another instance of `upload_new_wheel()` is being called at the same time (and hasn't removed the old package yet),
             so the latest instance should stop (which is what this test is testing)
    """
    env = "mock_env"
    wheel_name = "odw_test_wheel.whl"
    mock_odw_wheels = [
        {
            "name": "odw-1.0.0-some_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        },
        {
            "name": "odw-1.0.0-some_other_commit-py3-none-any.whl",
            "properties": {
                "uploadedTimestamp": "2025-08-01T13:58:54.3370407+00:00"
            }
        }
    ]
    with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value = mock_odw_wheels):
            with pytest.raises(ConcurrentWheelUploadException):
                ODWPackageDeployer().upload_new_wheel(env, wheel_name)


def test_upload_new_wheel__with_duplicate_existing_package():
    """
        Attempting to upload a package which already exists in the workspace should abort the operation to save time
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
    with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager"):
        with mock.patch.object(ODWPackageDeployer, "get_existing_odw_wheels", return_value=mock_odw_wheels):
            with mock.patch("pipelines.scripts.util.synapse_workspace_manager.SynapseWorkspaceManager"):
                with mock.patch.object(SynapseWorkspaceManager, "upload_workspace_package", return_value=None):
                    with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=None):
                        with mock.patch.object(SynapseWorkspaceManager, "update_spark_pool", return_value=None):
                            with mock.patch.object(SynapseWorkspaceManager, "remove_workspace_package", return_value=None):
                                ODWPackageDeployer().upload_new_wheel(env, wheel_name)
                                assert not SynapseWorkspaceManager.upload_workspace_package.called
                                assert not SynapseWorkspaceManager.get_spark_pool.called
                                assert not SynapseWorkspaceManager.update_spark_pool.called
                                assert not SynapseWorkspaceManager.remove_workspace_package.called
