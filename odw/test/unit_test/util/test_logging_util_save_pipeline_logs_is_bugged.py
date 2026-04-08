import logging
from unittest import mock

from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil


def _build_logging_util_instance():
    inst = object.__new__(LoggingUtil)
    inst.instrumentation_key = "test-instrumentation-key"
    inst.pipelinejobid = "test-pipeline-job-id"
    inst.logger = logging.getLogger("test_logging_util_save_pipeline_logs")
    return inst


def test_save_pipeline_logs__success_path_logs_status_code():
    """
    Verifies that when requests.post succeeds, the status code is logged correctly
    """
    logging_util = _build_logging_util_instance()

    mock_response = mock.Mock()
    mock_response.status_code = 200

    with mock.patch("odw.core.util.logging_util.requests.post", return_value=mock_response):
        with mock.patch.object(logging_util, "log_info", return_value=None) as mock_log_info:
            logging_util.save_pipeline_logs(
                payload={"table_name": "sb_service_user", "insert_count": 10},
                event_name="Master_Pipeline_Logs_v2",
            )

        mock_log_info.assert_called_once_with("Telemetry sent: 200")


def test_save_pipeline_logs__success_path_calls_requests_post():
    """
    Verifies that requests.post is called correctly when saving pipeline logs
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


def test_save_pipeline_logs__dataframe_payload_triggers_exception_handling():
    """
    Verifies that when a DataFrame causes telemetry submission to fail,
    the exception is logged correctly as a single formatted string
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
        # The DataFrame would be sanitised to a string representation
        # but we simulate an exception being raised
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

    assert mock_log_info.call_count == 1
    call_args = mock_log_info.call_args[0]
    logged_message = call_args[0]

    assert logged_message.startswith("Failed to send telemetry:")
    assert "DataFrame is not JSON serializable" in logged_message


def test_save_pipeline_logs__dataframe_payload_handles_exception_gracefully():
    """
    Verifies that when a DataFrame causes telemetry submission to fail,
    the exception is handled gracefully without raising additional errors
    """
    logging_util = _build_logging_util_instance()
    spark = PytestSparkSessionUtil().get_spark_session()

    df = spark.createDataFrame([{"id": 1, "name": "John Doe"}])

    with mock.patch(
        "odw.core.util.logging_util.requests.post",
        side_effect=TypeError("Object of type DataFrame is not JSON serializable"),
    ):
        # Should not raise an exception - it should be caught and logged
        logging_util.save_pipeline_logs(
            payload={"bad_dataframe": df},
            event_name="Master_Pipeline_Logs_v2",
        )


def test_save_pipeline_logs__serialisable_payload_is_fine_once_log_info_is_not_the_real_method():
    """
    This isolates the problem even more
    With a normal serialisable payload and log_info mocked out
    save_pipeline_logs completes fine
    That proves the 'success path' issue is specifically the bad log_info call
    not the normal telemetry payload structure itself
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
                    "activity_type": "ServiceBusStandardisationProcess",
                },
                event_name="Master_Pipeline_Logs_v2",
            )

    mock_post.assert_called_once()
