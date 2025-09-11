from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
import odw.core.util.logging_util
import pytest
import mock
from logging import Logger
import logging


def get_new_logging_instance():
    with mock.patch.object(LoggingUtil, "__new__", return_value=object.__new__(LoggingUtil)):
        with mock.patch("odw.core.util.logging_util.configure_azure_monitor"):
            return LoggingUtil()


def test_logging_util_is_a_singleton():
    with mock.patch.object(LoggingUtil, "_initialise", return_value=None):
        instance_a = LoggingUtil()
        instance_b = LoggingUtil()
        assert id(instance_a) == id(instance_b), "Expected the same object to be returned by the constructor, but different objects were returned"
        LoggingUtil._initialise.assert_called_once()


def test_logging_util__initialise():
    LoggingUtil._INSTANCE = None
    mock_mssparkutils_context = {"pipelinejobid": "some_guid", "isForPipeline": True}
    with mock.patch("notebookutils.mssparkutils.runtime.context", mock_mssparkutils_context):
        with mock.patch.object(notebookutils.mssparkutils.credentials, "getSecretWithLS", return_value="some_connection_string;blah;blah"):
            with mock.patch("odw.core.util.logging_util.configure_azure_monitor"):
                logging_util_inst = LoggingUtil()
                assert logging_util_inst.pipelinejobid == "some_guid"
                assert isinstance(logging_util_inst.logger, Logger)
                odw.core.util.logging_util.configure_azure_monitor.assert_called_once()


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
            with mock.patch("odw.core.util.logging_util.configure_azure_monitor"):
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
