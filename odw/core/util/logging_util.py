import logging
import functools
import uuid
from notebookutils import mssparkutils
from tenacity import retry, wait_exponential, stop_after_delay
from tenacity.before_sleep import before_sleep_nothing
from typing import Dict, Any, Literal
from azure.identity import AzureCliCredential
import requests
import datetime
import traceback


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
        self._LOGGING_INITIALISED = False
        self.app_insights_endpoint = "https://uksouth-1.in.applicationinsights.azure.com/v2/track"
        self.pipelinejobid = (
            mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()
        )
        self.logger = logging.getLogger()
        self.setup_logging()

    def _send_data_to_app_insights(self, log_data_type: Literal["TraceData", "ExceptionData"], payload: Dict[str, Any]):
        """
        Post data to log analytics
        """
        payload = {
            "name": log_data_type,
            "time": datetime.datetime.now().isoformat() + "Z",
            "iKey": self.instrumentation_key,
            "data": payload
        }
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key
        }
        resp = requests.post(self.app_insights_endpoint, json=payload, headers=headers)
        print(resp)
        1 + "a"

    def log_info(self, msg: str):
        """
        Log an information message
        """
        message = f"{self.pipelinejobid} : {msg}"
        self.logger.info(message)
        self._send_data_to_app_insights(
            "TraceData",
            {
                "baseType": "TraceData",
                "baseData": {
                    "message": message,
                    "severityLevel": 1
                }
            }
        )

    def log_error(self, msg: str):
        """
        Log an error message string
        """
        error = f"{self.pipelinejobid} : {msg}"
        self.logger.error(error)
        self._send_data_to_app_insights(
            "ExceptionData",
            {
                "baseType": "ExceptionData",
                "baseData": {
                    "exceptions": [
                        {
                            "errorClass": "Exception",
                            "message": error,
                            "hasFullStack": False,
                            "stack": None
                        }
                    ],
                    "severityLevel": 3
                }
            }
        )

    def log_exception(self, ex: Exception):
        """
        Log an exception
        """
        exception = f"{self.pipelinejobid} : {ex}"
        self.logger.exception(exception)
        # Get the stack trace of the exception
        stack_trace = None
        try:
            raise exception
        except exception.__class__:
            stack_trace = traceback.format_exc()
        self._send_data_to_app_insights(
            "ExceptionData",
            {
                "baseType": "ExceptionData",
                "baseData": {
                    "exceptions": [
                        {
                            "errorClass": ex.__class__.__name__,
                            "message": exception,
                            "hasFullStack": True if stack_trace else False,
                            "stack": stack_trace
                        }
                    ],
                    "severityLevel": 3
                }
            }
        )

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_delay(20), reraise=True, before_sleep=before_sleep_nothing)
    def setup_logging(self, force=False):
        """
        Initialise logging to Azure App Insights
        """
        if self._LOGGING_INITIALISED and not force:
            self.log_info("Logging already initialised.")
            return
        key = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        if not key:
            raise RuntimeError("The credential returned by mssparkutils.credentials.getSecretWithLS was blank or None")
        conn_string = key.split(";")
        self.instrumentation_key = conn_string[0].split("=")[1]
        self.api_key = conn_string[3].split("=")[1]

        self.logger.setLevel(logging.INFO)
        self._LOGGING_INITIALISED = True
        self.log_info("Logging initialised.")

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
