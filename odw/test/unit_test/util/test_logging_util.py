from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.logging_util import LoggingUtil
from opentelemetry.sdk._logs import LoggerProvider
import pytest
import mock
from logging import Logger


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
    pass


def test_logging_util__log_error():
    pass


def test_logging_util__log_exception():
    pass


def test_logging_util__setup_logging():
    pass


def test_logging_util__flush_logging():
    pass


def test_logging_util__logging_to_appins():
    pass


def test_logging_util__logging_to_appins__as_decorator():
    pass
