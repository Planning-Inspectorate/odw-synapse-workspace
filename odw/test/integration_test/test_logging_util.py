from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.config import TEST_CONFIG
from azure.identity import AzureCliCredential
import requests
from uuid import uuid4
import mock
import json


APP_INSIGHTS_TOKEN = AzureCliCredential().get_token("https://api.applicationinsights.io/.default").token


def query_app_insights(app_id: str, expected_message: str):
    query = f"traces | where message == '{expected_message}'"
    payload = {
        "query": query,
        "timespan": "PT30M"
    }
    resp = requests.post(
        f"https://api.applicationinsights.io/v1/apps/{app_id}/query",
        json=payload,
        headers={"Authorization": f"Bearer {APP_INSIGHTS_TOKEN}"}
    )
    resp_json = resp.json()
    return resp_json


#app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
#app_insights_app_id = app_insights_connection_string.split("ApplicationId=")[1]

#res = query_app_insights(app_insights_app_id)
#app_insights_traces = res.get("tables", [dict()])[0].get("rows", [])
#print("num rows: ", len(app_insights_traces))

def test_logs_sent_to_app_insights():
    @LoggingUtil.logging_to_appins
    def some_test_function(mock_arg_a: str, mock_arg_b: str):
        return f"some_test_function says '{mock_arg_a}' and '{mock_arg_b}'"

    job_uuid = uuid4()
    mock_mssparkutils_context = {
        "pipelinejobid": job_uuid,
        "isForPipeline": True
    }
    app_insights_connection_string = TEST_CONFIG["APP_INSIGHTS_CONNECTION_STRING"]
    app_insights_app_id = app_insights_connection_string.split("ApplicationId=")[1]
    with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
        with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value=app_insights_connection_string):
            some_test_function("Hello", mock_arg_b="There")
            expected_logging_initialised_message = f"{job_uuid} : Logging initialised."
            app_insights_traces_response = query_app_insights(app_insights_app_id, expected_logging_initialised_message)
            app_insights_traces = app_insights_traces_response.get("tables", [dict()])[0].get("rows", [])
            assert app_insights_traces
