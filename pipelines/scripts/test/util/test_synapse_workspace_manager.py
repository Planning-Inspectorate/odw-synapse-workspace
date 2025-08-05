from pipelines.scripts.util.synapse_workspace_manager import SynapseWorkspaceManager
from pipelines.scripts.util.exceptions import MaxWaitTimeNeededException
from pipelines.scripts.util.util import Util
import pytest
import mock
import requests
import json
import time
import math


SUBSCRIPTION_ID = "some_subscription"
RESOURCE_GROUP_NAME = "some_resource_group"
SYNAPSE_WORKSPACE_NAME = "some_synapse_workspace"
SYNAPSE_ENDPOINT = (
    f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/"
    f"Microsoft.Synapse/workspaces/{SYNAPSE_WORKSPACE_NAME}"
)


def test_get_workspace_packages():
    with mock.patch.object(requests, "get") as mock_get:
        mock_get.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            resp = SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).get_workspace_packages()
            requests.get.asset_called_once_with(
                f"{SYNAPSE_ENDPOINT}/libraries?api-version=2021-06-01",
                headers={"Authorization": f"Bearer some_token"}
            )
            assert resp == "get_response"


def test_get_workspace_packages_with_exception():
    with mock.patch.object(requests, "get") as mock_get:
        mock_get.return_value.json.side_effect = json.JSONDecodeError("", "", 0)
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with pytest.raises(ValueError):
                SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).get_workspace_packages()


def test_upload_workspace_package():
    package_path = "dist/some_package.whl"
    mock_packages = [
        {"name": "some_package.whl", "properties": {"provisioningStatus": "Succeeded"}}
    ]
    expected_cli_call_args = [
        "az",
        "synapse",
        "workspace-package",
        "upload",
        "--workspace-name",
        SYNAPSE_WORKSPACE_NAME,
        "--package",
        package_path,
        "--no-progress"
    ]
    mock_response = {"resp": "mock_return_value"}
    with mock.patch.object(Util, "run_az_cli_command", return_value=json.dumps(mock_response)):
        with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=mock_packages):
            with mock.patch.object(time, "sleep"):
                resp = SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).upload_workspace_package(package_path)
                Util.run_az_cli_command.assert_called_once_with(expected_cli_call_args)
                assert resp == mock_response


def test_upload_workspace_package_exceeded_wait_time():
    package_path = "dist/a_missing_package.whl"
    mock_packages = [
        {"name": "some_package.whl", "properties": {"provisioningStatus": "Queued"}}
    ]
    max_wait_time = 300  # Seconds
    retry_delay = 20  # Seconds
    expected_sleep_call_count = math.ceil(max_wait_time // retry_delay)
    mock_response = {"resp": "mock_return_value"}
    expected_sleep_calls = [mock.call(retry_delay) for x in range(0, expected_sleep_call_count)]
    with mock.patch.object(Util, "run_az_cli_command", return_value=json.dumps(mock_response)):
        with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=mock_packages):
            with mock.patch.object(time, "sleep"):
                with pytest.raises(MaxWaitTimeNeededException):
                    SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).upload_workspace_package(package_path)
                time.sleep.assert_has_calls(expected_sleep_calls)


def test_remove_workspace_package():
    package_name = "some_package.whl"
    mock_packages = [
        {"name": "some_other_package.whl"}
    ]
    expected_cli_call_args = [
        "az",
        "synapse",
        "workspace-package",
        "delete",
        "--workspace-name",
        SYNAPSE_WORKSPACE_NAME,
        "--package",
        package_name,
        "--no-wait",
        "-y"
    ]
    with mock.patch.object(Util, "run_az_cli_command"):
        with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=mock_packages):
            with mock.patch.object(time, "sleep"):
                SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).remove_workspace_package(package_name)
                Util.run_az_cli_command.assert_called_once_with(expected_cli_call_args)


def test_remove_workspace_package_exceeded_wait_time():
    package_path = "some_package.whl"
    mock_packages = [
        {"name": "some_package.whl"}
    ]
    max_wait_time = 600  # Seconds
    retry_delay = 20  # Seconds
    expected_sleep_call_count = math.ceil(max_wait_time // retry_delay)
    expected_sleep_calls = [mock.call(retry_delay) for x in range(0, expected_sleep_call_count)]
    with mock.patch.object(Util, "run_az_cli_command"):
        with mock.patch.object(SynapseWorkspaceManager, "get_workspace_packages", return_value=mock_packages):
            with mock.patch.object(time, "sleep"):
                with pytest.raises(MaxWaitTimeNeededException):
                    SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).remove_workspace_package(package_path)
                time.sleep.assert_has_calls(expected_sleep_calls)


def test_get_spark_pool():
    pool_name = "some_spark_pool"
    with mock.patch.object(requests, "get") as mock_get:
        mock_get.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            resp = SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).get_spark_pool(pool_name)
            requests.get.asset_called_once_with(
                f"{SYNAPSE_ENDPOINT}/bigDataPools/{pool_name}?api-version=2021-06-01",
                headers={"Authorization": f"Bearer some_token"}
            )
            assert resp == {"value": "get_response"}


def test_get_spark_pool_with_azure_error():
    pool_name = "some_spark_pool"
    with mock.patch.object(requests, "get") as mock_get:
        mock_get.return_value.json.return_value = {"error": "some error message"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with pytest.raises(ValueError):
                SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).get_spark_pool(pool_name)


def test_get_spark_pool_with_exception():
    pool_name = "some_spark_pool"
    with mock.patch.object(requests, "get") as mock_get:
        mock_get.return_value.json.side_effect = json.JSONDecodeError("", "", 0)
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with pytest.raises(ValueError):
                SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).get_spark_pool(pool_name)


def test_update_spark_pool():
    pool_name = "some_spark_pool"
    new_pool_json = "new pool json"
    mock_spark_pool_response = {
        "properties": {
            "provisioningState": "Succeeded"
        }
    }
    with mock.patch.object(requests, "put") as mock_put:
        mock_put.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool_response):
                resp = SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).update_spark_pool(
                    pool_name,
                    new_pool_json
                )
                requests.put.asset_called_once_with(
                    f"{SYNAPSE_ENDPOINT}/bigDataPools/{pool_name}?api-version=2021-06-01",
                    headers={"Authorization": f"Bearer some_token"}
                )
                assert resp == {"value": "get_response"}


def test_update_spark_pool_with_json_decode_exception():
    pool_name = "some_spark_pool"
    new_pool_json = "new pool json"
    mock_spark_pool_response = {
        "properties": {
            "provisioningState": "Succeeded"
        }
    }
    with mock.patch.object(requests, "put") as mock_put:
        mock_put.return_value.json.side_effect = json.JSONDecodeError("", "", 0)
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool_response):
                with pytest.raises(ValueError):
                    SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).update_spark_pool(
                        pool_name,
                        new_pool_json
                    )


def test_update_spark_pool_with_provisioning_failed_exception():
    pool_name = "some_spark_pool"
    new_pool_json = "new pool json"
    mock_spark_pool_response = {
        "properties": {
            "provisioningState": "Failed"  # Provisioning failed, which will cause an exception to be raised
        }
    }
    with mock.patch.object(requests, "put") as mock_put:
        mock_put.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool_response):
                with pytest.raises(ValueError):
                    SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).update_spark_pool(
                        pool_name,
                        new_pool_json
                    )


def test_update_spark_pool_with_provisioning_canceled_exception():
    pool_name = "some_spark_pool"
    new_pool_json = "new pool json"
    mock_spark_pool_response = {
        "properties": {
            "provisioningState": "Canceled"  # Provisioning canceled, which will cause an exception to be raised
        }
    }
    with mock.patch.object(requests, "put") as mock_put:
        mock_put.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool_response):
                with pytest.raises(ValueError):
                    SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).update_spark_pool(
                        pool_name,
                        new_pool_json
                    )


def test_update_sparkpool_exceeded_wait_time():
    pool_name = "some_spark_pool"
    new_pool_json = "new pool json"
    mock_spark_pool_response = {
        "properties": {
            "provisioningState": "Updating"
        }
    }
    max_wait_time = 3000  # Seconds
    retry_delay = 60  # Seconds
    expected_sleep_call_count = math.ceil(max_wait_time // retry_delay)
    expected_sleep_calls = [mock.call(retry_delay) for x in range(0, expected_sleep_call_count)]
    with mock.patch.object(requests, "put") as mock_put:
        mock_put.return_value.json.return_value = {"value": "get_response"}
        with mock.patch.object(SynapseWorkspaceManager, "_get_token", return_value="some_token"):
            with mock.patch.object(SynapseWorkspaceManager, "get_spark_pool", return_value=mock_spark_pool_response):
                with mock.patch.object(time, "sleep"):
                    with pytest.raises(MaxWaitTimeNeededException):
                        SynapseWorkspaceManager(SYNAPSE_WORKSPACE_NAME, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME).update_spark_pool(
                            pool_name,
                            new_pool_json
                        )
                    time.sleep.assert_has_calls(expected_sleep_calls)
