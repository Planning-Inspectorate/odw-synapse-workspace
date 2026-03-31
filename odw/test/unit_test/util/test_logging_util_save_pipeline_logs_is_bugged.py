import logging
from unittest import mock
import pytest
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil


def _build_logging_util_instance():
    inst = object.__new__(LoggingUtil)
    inst.instrumentation_key = "test-instrumentation-key"
    inst.pipelinejobid = "test-pipeline-job-id"
    inst.logger = logging.getLogger("test_logging_util_save_pipeline_logs")
    return inst


def test_save_pipeline_logs__success_path_currently_raises_typeerror_because_log_info_is_called_with_two_args():
    """
    Proves the current implementation is broken even when requests.post succeeds
    Root cause:
        self.log_info("Telemetry sent:", response.status_code)
    log_info only accepts one message argument so this raises
        TypeError: ... takes 2 positional arguments but 3 were given
    """
    logging_util = _build_logging_util_instance()

    mock_response = mock.Mock()
    mock_response.status_code = 200

    with mock.patch("odw.core.util.logging_util.requests.post", return_value=mock_response):
        with pytest.raises(TypeError, match="positional arguments"):
            logging_util.save_pipeline_logs(
                payload={"table_name": "sb_service_user", "insert_count": 10},
                event_name="Master_Pipeline_Logs_v2",
            )


def test_save_pipeline_logs__success_path_proves_log_info_is_called_with_two_arguments():
    """
    Proves exactly how the success path is currently calling log_info
    We patch log_info with a Mock so the call does not fail then assert the incorrect call signature
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

    # This proves the current implementation is passing two arguments to log_info
    mock_log_info.assert_called_once_with("Telemetry sent:", 200)


def test_save_pipeline_logs__dataframe_payload_reaches_requests_post_and_triggers_serialisation_failure_path():
    """
    Proves a DF in the payload can be the thing that breaks telemetry submission
    We simulate what requests/json serialisation would do when a Spark DF is in the payload
    Then we assert that the except block currently tries to log using the same broken two arg style
        self.log_info("Failed to send telemetry:", e)
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
        # Prove the DataFrame really is in the telemetry payload path
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

    # This proves the failure path is also using the broken two argument call style
    assert mock_log_info.call_count == 1
    call_args = mock_log_info.call_args[0]

    assert call_args[0] == "Failed to send telemetry:"
    assert isinstance(call_args[1], TypeError)
    assert "DataFrame is not JSON serializable" in str(call_args[1])


def test_save_pipeline_logs__dataframe_payload_with_real_log_info_raises_typeerror_from_except_block():
    """
    Proves that when a DF causes telemetry submission to fail
    the except block then makes things worse by calling log_info incorrectly
    So instead of cleanly logging the serialisation problem
    the method raises TypeError because of the bad log_info call
    """
    logging_util = _build_logging_util_instance()
    spark = PytestSparkSessionUtil().get_spark_session()

    df = spark.createDataFrame([{"id": 1, "name": "John Doe"}])

    with mock.patch(
        "odw.core.util.logging_util.requests.post",
        side_effect=TypeError("Object of type DataFrame is not JSON serializable"),
    ):
        with pytest.raises(TypeError, match="positional arguments"):
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