import logging
from azure.monitor.opentelemetry import configure_azure_monitor
import functools
import uuid
from notebookutils import mssparkutils


class LoggingUtil:
    """
    Singleton logging utility class that provides functionality to send logs to app insights.

    Example usage
    ```
    from odw.core.util.logging_util import LoggingUtil
    LoggingUtil().log_info("Some logging message)
    @LoggingUtil.logging_to_appins
    def my_function_that_will_have_automatic_logging_applied():
        pass
    ```

    This is based on
    https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/monitor/azure-monitor-opentelemetry-exporter/samples/logs/sample_log.py
    """

    _INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls._INSTANCE:
            cls._INSTANCE = super(LoggingUtil, cls).__new__(cls, *args, **kwargs)
            cls._INSTANCE._initialise()
        return cls._INSTANCE

    def _initialise(self):
        """
        Create a `LoggingUtil` instance. Only 1 instance is ever created, which is reused.

        __init__ cannot be used because it is always called by __new__, even if cls._INSTANCE is not None
        """
        self.pipelinejobid = (
            mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()
        )
        self.logger_name_space = "odw_logs"
        self.logger = logging.getLogger(self.logger_name_space)

        app_insights_connection_string = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        if not app_insights_connection_string:
            raise RuntimeError("The credential returned by mssparkutils.credentials.getSecretWithLS was blank or None")
        configure_azure_monitor(logger_name=self.logger_name_space, connection_string=app_insights_connection_string)

        self.logger.setLevel(logging.INFO)
        self.log_info("Logging initialised.")

    def log_info(self, msg: str):
        """
        Log an information message
        """
        self.logger.info(f"{self.pipelinejobid} : {msg}")

    def log_error(self, msg: str):
        """
        Log an error message string
        """
        self.logger.error(f"{self.pipelinejobid} : {msg}")

    def log_exception(self, ex: Exception):
        """
        Log an exception
        """
        self.logger.exception(f"{self.pipelinejobid} : {ex}")

    @classmethod
    def logging_to_appins(cls, func):
        """
        Decorator that adds extra logging to function calls

        Example usage
        ```
        @LoggingUtil.logging_to_appins
        def my_function_that_will_be_logged(param_a, param_b):
            ...
        ```

        ```
        @classmethod
        @LoggingUtil.logging_to_appins
        def my_class_method_that_will_be_logged(cls, param_a, param_b):
            ...
        ```
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logging_util = LoggingUtil()
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            logging_util.log_info(f"Function {func.__name__} called with args: {', '.join(args_repr + kwargs_repr)}")
            try:
                return func(*args, **kwargs)
            except mssparkutils.handlers.notebookHandler.NotebookExit as e:
                logging_util.log_info(f"Notebook exited: {e}")
                mssparkutils.notebook.exit(e)
            except Exception as e:
                logging_util.log_exception(e)
                raise

        return wrapper
