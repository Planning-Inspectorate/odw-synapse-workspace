import logging
import functools
import uuid
from notebookutils import mssparkutils
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from tenacity import retry, wait_exponential, stop_after_delay, before_sleep


class LoggingUtil():
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
        return cls._INSTANCE

    def __init__(self):
        """
            Create a `LoggingUtil` instance. Only 1 instance is ever created, which is reused
        """
        self.LOGGER_PROVIDER = LoggerProvider()
        self._LOGGING_INITIALISED = False
        self.pipelinejobid = mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()
        self.logger = logging.getLogger()
        for h in list(self.logger.handlers):
            if isinstance(h, LoggingHandler):
                self.logger.removeHandler(h)
        self.setup_logging()
        self.flush_logging()

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

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_delay(20), reraise=True, before_sleep=before_sleep)
    def setup_logging(self, force=False):
        """
            Initialise logging to Azure App Insights
        """
        if self._LOGGING_INITIALISED and not force:
            self.log_info("Logging already initialised.")
            return
        key = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        conn_string = key.split(";")[0]

        set_logger_provider(self.LOGGER_PROVIDER)
        exporter = AzureMonitorLogExporter.from_connection_string(conn_string)
        self.LOGGER_PROVIDER.add_log_record_processor(BatchLogRecordProcessor(exporter, schedule_delay_millis=5000))

        if not any(isinstance(h, LoggingHandler) for h in self.logger.handlers):
            self.logger.addHandler(LoggingHandler())

        if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            self.logger.addHandler(logging.StreamHandler())

        self.logger.setLevel(logging.INFO)
        self._LOGGING_INITIALISED = True
        self.log_info("Logging initialised.")

    def flush_logging(self, timeout_seconds: int = 60):
        """
            Attempt to flush logs to Azure App Insights
        """
        try:
            self.LOGGER_PROVIDER.force_flush(timeout_millis=timeout_seconds * 1000)
        except Exception as e:
            print(f"Flush failed: {e}")

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
