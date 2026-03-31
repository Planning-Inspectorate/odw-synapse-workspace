import logging
from unittest import mock
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil


def _build_logging_util_instance():
    """
    Build a lightweight LoggingUtil instance without calling the real singleton initialisation
    because the real initialisation depends on mssparkutils/secrets/App Insights setup
    """
    inst = object.__new__(LoggingUtil)
    inst.instrumentation_key = "test-instrumentation-key"
    inst.pipelinejobid = "test-pipeline-job-id"
    inst.logger = logging.getLogger("test_logging_util_save_pipeline_logs")
    return inst


def test_save_pipeline_logs__success_path_logs_single_formatted_message():
    """
    After the fix (discussed with Stef) the success path should not raise
    It should log a single formatted success message
    """
    logging_util = _build_logging_util_instance()

    mock_response = mock.Mock()
    mock_response.status_code = 200

    with mock.patch("odw.core.util.logging_util.requests.post", return_value=mock_response) as mock_post:
        with mock.patch.object(logging_util, "log_info", return_value=None) as mock_log_info:
            logging_util.save_pipeline_logs(
                payload={"table_name": "sb_service_user", "insert_count": 10},
                event_name="Master_Pipeline_Logs_v2",
            )

    mock_post.assert_called_once()
    mock_log_info.assert_called_once_with("Telemetry sent: 200")


def test_save_pipeline_logs__success_path_does_not_raise_with_real_log_info():
    """
    After the fix even with the real log_info method
    a normal serialisable payload should not raise
    """
    logging_util = _build_logging_util_instance()

    mock_response = mock.Mock()
    mock_response.status_code = 200

    with mock.patch("odw.core.util.logging_util.requests.post", return_value=mock_response):
        logging_util.save_pipeline_logs(
            payload={
                "table_name": "sb_service_user",
                "insert_count": 10,
                "activity_type": "ServiceBusStandardisationProcess",
            },
            event_name="Master_Pipeline_Logs_v2",
        )


def test_save_pipeline_logs__dataframe_payload_reaches_requests_post_and_logs_single_formatted_error_message():
    """
    Proves that when a DF in the payload causes telemetry submission to fail
    the except block now logs a single formatted message instead of raising TypeError
    """
    logging_util = _build_logging_util_instance()
    spark = PytestSparkSessionUtil().get_spark_session()

    df = spark.createDataFrame(
        [
            {"id": 1, "name": "John Doe"},
            {"id": 2, "name": "Jane Smith"},
        ]
    )

    def fake_post(*args, **kwargs):
        assert "json" in kwargs
        request_payload = kwargs["json"]

        properties = request_payload["data"]["baseData"]["properties"]
        assert "bad_dataframe" in properties
        assert str(type(properties["bad_dataframe"])).endswith("DataFrame'>")

        raise TypeError("Object of type DataFrame is not JSON serializable")

    with mock.patch("odw.core.util.logging_util.requests.post", side_effect=fake_post):
        with mock.patch.object(logging_util, "log_info", return_value=None) as mock_log_info:
            logging_util.save_pipeline_logs(
                payload={
                    "table_name": "sb_service_user",
                    "bad_dataframe": df,
                },
                event_name="Master_Pipeline_Logs_v2",
            )

    mock_log_info.assert_called_once()
    logged_message = mock_log_info.call_args[0][0]

    assert logged_message.startswith("Failed to send telemetry: ")
    assert "DataFrame is not JSON serializable" in logged_message


def test_save_pipeline_logs__dataframe_payload_with_real_log_info_does_not_raise_typeerror_from_except_block():
    """
    After the fix if requests.post raises because the payload is not serializable
    save_pipeline_logs should not crash again while trying to log the failure
    """
    logging_util = _build_logging_util_instance()
    spark = PytestSparkSessionUtil().get_spark_session()

    df = spark.createDataFrame([{"id": 1, "name": "John Doe"}])

    with mock.patch(
        "odw.core.util.logging_util.requests.post",
        side_effect=TypeError("Object of type DataFrame is not JSON serializable"),
    ):
        logging_util.save_pipeline_logs(
            payload={"bad_dataframe": df},
            event_name="Master_Pipeline_Logs_v2",
        )


def test_save_pipeline_logs__requests_post_receives_expected_telemetry_shape_for_serialisable_payload():
    """
    After the fix verify the telemetry payload structure sent to requests.post
    still has the expected top level kind of shape
    """
    logging_util = _build_logging_util_instance()

    mock_response = mock.Mock()
    mock_response.status_code = 200

    with mock.patch("odw.core.util.logging_util.requests.post", return_value=mock_response) as mock_post:
        with mock.patch.object(logging_util, "log_info", return_value=None):
            logging_util.save_pipeline_logs(
                payload={
                    "table_name": "sb_service_user",
                    "insert_count": 10,
                },
                event_name="Master_Pipeline_Logs_v2",
            )

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args

    assert "json" in kwargs
    sent_payload = kwargs["json"]

    assert sent_payload["name"] == "Microsoft.ApplicationInsights.Event"
    assert sent_payload["iKey"] == "test-instrumentation-key"
    assert sent_payload["data"]["baseType"] == "EventData"
    assert sent_payload["data"]["baseData"]["name"] == "Master_Pipeline_Logs_v2"
    assert sent_payload["data"]["baseData"]["properties"]["table_name"] == "sb_service_user"
    assert sent_payload["data"]["baseData"]["properties"]["insert_count"] == 10