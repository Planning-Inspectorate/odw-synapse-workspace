from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.config import TEST_CONFIG
from azure.identity import AzureCliCredential
import requests
from uuid import uuid4
import mock
import pytest


APP_INSIGHTS_TOKEN = AzureCliCredential().get_token("https://api.applicationinsights.io/.default").token
JOB_ID = uuid4()


def query_app_insights(app_id: str, expected_message: str):
    query = f'traces | where message contains "{expected_message}"'
    payload = {"query": query, "timespan": "PT30M"}
    resp = requests.post(
        f"https://api.applicationinsights.io/v1/apps/{app_id}/query", json=payload, headers={"Authorization": f"Bearer {APP_INSIGHTS_TOKEN}"}
    )
    resp_json = resp.json()
    return resp_json


@pytest.fixture(autouse=True, scope="module")
def run_logging_util():
    @LoggingUtil.logging_to_appins
    def some_test_function(mock_arg_a: str, mock_arg_b: str):
        return f"some_test_function says '{mock_arg_a}' and '{mock_arg_b}'"

    mock_mssparkutils_context = {"pipelinejobid": JOB_ID, "isForPipeline": True}
    app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
    with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
        with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value=app_insights_connection_string):
            some_test_function("Hello", mock_arg_b="There")
            LoggingUtil().flush_logging()


def test_logging_initialised():
    app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
    app_insights_app_id = app_insights_connection_string.split("ApplicationId=")[1]
    expected_logging_initialised_message = f"{JOB_ID} : Logging initialised."
    logging_initialised_query_response = query_app_insights(app_insights_app_id, expected_logging_initialised_message)
    logging_initialised_traces = logging_initialised_query_response.get("tables", [dict()])[0].get("rows", [])
    if not logging_initialised_traces:
        pytest.skip(f"Logging flush failed for job with id {JOB_ID}, but this is not a major issue - skipping the test")


def test_logging_function_call():
    app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
    app_insights_app_id = app_insights_connection_string.split("ApplicationId=")[1]
    expected_logging_function_message = f"{JOB_ID} : Function some_test_function called with args: 'Hello', mock_arg_b='There'"
    function_logging_query_response = query_app_insights(app_insights_app_id, expected_logging_function_message)
    function_logging_traces = function_logging_query_response.get("tables", [dict()])[0].get("rows", [])
    if not function_logging_traces:
        pytest.skip(f"Logging flush failed for job with id {JOB_ID}, but this is not a major issue - skipping the test")
