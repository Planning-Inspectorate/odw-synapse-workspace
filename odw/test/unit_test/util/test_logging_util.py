from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from opentelemetry.sdk._logs import LoggerProvider
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
import pytest
import mock
from logging import Logger
import logging


def get_new_logging_instance():
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "__new__", return_value=object.__new__(LoggingUtil)):
            return LoggingUtil()


def test_logging_util_is_a_singleton():
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        instance_a = LoggingUtil()
        instance_b = LoggingUtil()
        assert id(instance_a) == id(instance_b), "Expected the same object to be returned by the constructor, but different objects were returned"


def test_logging_util__init():
    with mock.patch.object(LoggingUtil, "setup_logging", return_value=None):
        with mock.patch.object(LoggingUtil, "flush_logging", return_value=None):
            with mock.patch.object(Logger, "removeHandler", return_value=None):
                notebookutils.mssparkutils.runtime.context = {
                    "pipelinejobid": "some_guid",
                    "isForPipeline": True
                }
                logging_util_inst = LoggingUtil()
                assert isinstance(logging_util_inst.LOGGER_PROVIDER, LoggerProvider)
                assert logging_util_inst.pipelinejobid == "some_guid"
                assert isinstance(logging_util_inst.logger, Logger)


def test_logging_util__log_info():
    logging_util_inst = get_new_logging_instance()
    pipeline_guid = "some_pipeline_guid"
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = pipeline_guid
    with mock.patch.object(logging.Logger, "info", return_value=None):
        info_message = "some_info_message"
        logging_util_inst.log_info(info_message)
        logging.Logger.info.assert_called_once_with(f"{pipeline_guid} : {info_message}")


def test_logging_util__log_error():
    logging_util_inst = get_new_logging_instance()
    pipeline_guid = "some_pipeline_guid"
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = pipeline_guid
    with mock.patch.object(logging.Logger, "error", return_value=None):
        error_message = "some_error_message"
        logging_util_inst.log_error(error_message)
        logging.Logger.error.assert_called_once_with(f"{pipeline_guid} : {error_message}")


def test_logging_util__log_exception():
    logging_util_inst = get_new_logging_instance()
    pipeline_guid = "some_pipeline_guid"
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = pipeline_guid
    with mock.patch.object(logging.Logger, "exception", return_value=None):
        exception_message = Exception("some exception")
        logging_util_inst.log_exception(exception_message)
        logging.Logger.exception.assert_called_once_with(f"{pipeline_guid} : {exception_message}")


def test_logging_util__setup_logging():
    logging_util_inst = get_new_logging_instance()
    logging_util_inst.LOGGER_PROVIDER = LoggerProvider()
    logging_util_inst._LOGGING_INITIALISED = False
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = "some_pipeline_guid"
    notebookutils.mssparkutils.credentials.getSecretWithLS.return_value = "some_connection_string;blah;blah"
    mock_exporter = mock.MagicMock()
    mock_batch_log_record_processor = mock.MagicMock()
    with mock.patch.object(AzureMonitorLogExporter, "from_connection_string", return_value=mock_exporter):
        with mock.patch.object(LoggerProvider, "add_log_record_processor"):
            with mock.patch.object(BatchLogRecordProcessor, "__new__", return_value=mock_batch_log_record_processor):
                logging_util_inst.setup_logging()
                BatchLogRecordProcessor.__new__.assert_called_once_with(BatchLogRecordProcessor, mock_exporter, schedule_delay_millis=5000)
                LoggerProvider.add_log_record_processor.assert_called_once_with(mock_batch_log_record_processor)
                assert logging_util_inst.logger.level == logging.INFO
                assert logging_util_inst._LOGGING_INITIALISED


def test_logging_util__setup_logging__already_initialised():
    pass


@pytest.mark.skip(reason="It is not possible to mock mssparkutils in a multithreaded function call, so skipping this test")
def test_logging_util__flush_logging():
    logging_util_inst = get_new_logging_instance()
    logging_util_inst.LOGGER_PROVIDER = LoggerProvider()
    logging_util_inst._CURRENT_WORKER_POOLS_COUNT = 0
    logging_util_inst._MAX_WORKER_POOLS = 5
    with mock.patch.object(LoggerProvider, "force_flush", return_value=None):
        with mock.patch.object(LoggingUtil, "__init__", return_value=None):
            logging_util_inst.flush_logging()
            LoggerProvider.force_flush.assert_called_once()


def test_logging_util__logging_to_appins():
    pass


def test_logging_util__logging_to_appins__as_decorator():
    pass
