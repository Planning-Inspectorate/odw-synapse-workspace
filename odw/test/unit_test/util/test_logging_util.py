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
    with mock.patch.object(LoggingUtil, "__new__", return_value=object.__new__(LoggingUtil)):
        return LoggingUtil()


def test_logging_util_is_a_singleton():
    with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
        instance_a = LoggingUtil()
        instance_b = LoggingUtil()
        assert id(instance_a) == id(instance_b), "Expected the same object to be returned by the constructor, but different objects were returned"
        LoggingUtil._initialise.assert_called_once()


def test_logging_util__initialise():
    LoggingUtil._INSTANCE = None
    with mock.patch.object(LoggingUtil, "setup_logging", return_value=None):
        with mock.patch.object(LoggingUtil, "flush_logging", return_value=None):
            with mock.patch.object(Logger, "removeHandler", return_value=None):
                mock_mssparkutils_context = {"pipelinejobid": "some_guid", "isForPipeline": True}
                with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
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


@pytest.mark.parametrize(
    "test_case",
    [
        (False, False),  # Without force initialisation. i.e. If not initialised, then initialise
        (True, True),  # With forced initialisation. i.e. If already initialised and running with force, then initialise
    ],
)
def test_logging_util__setup_logging(test_case):
    logging_initialised = test_case[0]
    force_initialise = test_case[1]
    logging_util_inst = get_new_logging_instance()
    logging_util_inst.LOGGER_PROVIDER = LoggerProvider()
    logging_util_inst._LOGGING_INITIALISED = logging_initialised
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = "some_pipeline_guid"
    with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value="some_connection_string;blah;blah"):
        mock_exporter = mock.MagicMock()
        mock_batch_log_record_processor = mock.MagicMock()
        with mock.patch.object(AzureMonitorLogExporter, "from_connection_string", return_value=mock_exporter):
            with mock.patch.object(LoggerProvider, "add_log_record_processor"):
                with mock.patch.object(BatchLogRecordProcessor, "__new__", return_value=mock_batch_log_record_processor):
                    logging_util_inst.setup_logging(force_initialise)
                    BatchLogRecordProcessor.__new__.assert_called_once_with(BatchLogRecordProcessor, mock_exporter, schedule_delay_millis=5000)
                    LoggerProvider.add_log_record_processor.assert_called_once_with(mock_batch_log_record_processor)
                    assert logging_util_inst.logger.level == logging.INFO
                    assert logging_util_inst._LOGGING_INITIALISED


def test_logging_util__setup_logging__already_initialised():
    # I.e if initialised and not running with force, then skip initialisation
    logging_util_inst = get_new_logging_instance()
    logging_util_inst.LOGGER_PROVIDER = LoggerProvider()
    logging_util_inst._LOGGING_INITIALISED = True
    logging_util_inst.logger = logging.getLogger()
    logging_util_inst.pipelinejobid = "some_pipeline_guid"
    with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value="some_connection_string;blah;blah"):
        mock_exporter = mock.MagicMock()
        mock_batch_log_record_processor = mock.MagicMock()
        with mock.patch.object(AzureMonitorLogExporter, "from_connection_string", return_value=mock_exporter):
            with mock.patch.object(LoggerProvider, "add_log_record_processor"):
                with mock.patch.object(BatchLogRecordProcessor, "__new__", return_value=mock_batch_log_record_processor):
                    logging_util_inst.setup_logging()
                    assert not AzureMonitorLogExporter.from_connection_string.called
                    assert not LoggerProvider.add_log_record_processor.called
                    assert not BatchLogRecordProcessor.__new__.called


def test_logging_util__flush_logging():
    logging_util_inst = get_new_logging_instance()
    logging_util_inst.LOGGER_PROVIDER = LoggerProvider()
    with mock.patch.object(LoggerProvider, "force_flush", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            logging_util_inst.flush_logging()
            LoggerProvider.force_flush.assert_called_once()


def test_logging_util__logging_to_appins():
    @LoggingUtil.logging_to_appins
    def my_function():
        return "Hello world"

    args_repr = []
    kwargs_repr = []

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            resp = my_function()
            LoggingUtil.log_info.assert_called_once_with(f"Function my_function called with args: {', '.join(args_repr + kwargs_repr)}")
            assert resp == "Hello world"


def test_logging_util__logging_to_appins__with_args():
    @LoggingUtil.logging_to_appins
    def my_function_with_args(a, b, c):
        return f"Hello world ({a}, {b}, {c})"

    args_repr = ["1", "2"]
    kwargs_repr = ["c='bob'"]

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            resp = my_function_with_args(1, 2, c="bob")
            LoggingUtil.log_info.assert_called_once_with(f"Function my_function_with_args called with args: {', '.join(args_repr + kwargs_repr)}")
            assert resp == "Hello world (1, 2, bob)"


def test_logging_util__logging_to_appins__with_notebook_exception():
    notebook_exit_exception = notebookutils.mssparkutils.handlers.notebookHandler.NotebookExit("Some exception")

    @LoggingUtil.logging_to_appins
    def my_function_with_notebook_exception():
        raise notebook_exit_exception

    args_repr = []
    kwargs_repr = []

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            with mock.patch.object(notebookutils.mssparkutils.notebook.exit, "exit", return_value="notebook exit"):
                notebookutils.mssparkutils.notebook.exit.return_value = "notebook exit"
                my_function_with_notebook_exception()
                LoggingUtil.log_info.assert_has_calls(
                    [
                        mock.call(f"Function my_function_with_notebook_exception called with args: {', '.join(args_repr + kwargs_repr)}"),
                        mock.call("Notebook exited: Some exception"),
                    ]
                )
                notebookutils.mssparkutils.notebook.exit.assert_called_once_with(notebook_exit_exception)


def test_logging_util__logging_to_appins__with_exception():
    exception = Exception("Some exception")

    @LoggingUtil.logging_to_appins
    def my_function_with_exception():
        raise exception

    args_repr = []
    kwargs_repr = []

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "log_exception", return_value=None):
            with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
                with pytest.raises(Exception):
                    my_function_with_exception()
                LoggingUtil.log_info.assert_called_once_with(
                    f"Function my_function_with_exception called with args: {', '.join(args_repr + kwargs_repr)}"
                )
                LoggingUtil.log_exception.assert_called_once_with(exception)


def test_logging_util__logging_to_appins__with_instance_method():
    class MyClass:
        @LoggingUtil.logging_to_appins
        def my_function(self):
            return "Hello world"

    inst = MyClass()
    args_repr = [str(inst)]
    kwargs_repr = []

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            resp = inst.my_function()
            LoggingUtil.log_info.assert_called_once_with(f"Function my_function called with args: {', '.join(args_repr + kwargs_repr)}")
            assert resp == "Hello world"


def test_logging_util__logging_to_appins__with_class_method():
    class MyClass:
        @classmethod
        @LoggingUtil.logging_to_appins
        def my_function(cls):
            return "Hello world"

    args_repr = [str(MyClass)]
    kwargs_repr = []

    with mock.patch.object(LoggingUtil, "log_info", return_value=None):
        with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
            resp = MyClass.my_function()
            LoggingUtil.log_info.assert_called_once_with(f"Function my_function called with args: {', '.join(args_repr + kwargs_repr)}")
            assert resp == "Hello world"
