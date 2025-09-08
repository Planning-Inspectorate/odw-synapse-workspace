import logging
import functools
import uuid
from notebookutils import mssparkutils
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from tenacity import retry, wait_exponential, stop_after_delay
from tenacity.before_sleep import before_sleep_nothing
import threading
import multiprocessing
import sys
import time
import psutil
import os


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
    _MAX_FLUSH_PROCESSES = 3  # Limit concurrent flush processes
    _CURRENT_FLUSH_COUNT = 0
    _FLUSH_LOCK = threading.Lock()
    _MAX_LOG_MEMORY_MB = 50  # Max memory usage before forcing flush
    _LOG_COUNT_THRESHOLD = 1000  # Max log entries before checking memory

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
        self.LOGGER_PROVIDER = LoggerProvider()
        self._LOGGING_INITIALISED = False
        self.pipelinejobid = (
            mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()
        )
        self.logger = logging.getLogger()
        for h in list(self.logger.handlers):
            if isinstance(h, LoggingHandler):
                self.logger.removeHandler(h)
        
        # Initialize logging monitoring
        self._log_count = 0
        self._last_memory_check = time.time()
        self._flush_in_progress = False
        
        self.setup_logging()
        self.flush_logging()

    def log_info(self, msg: str):
        """
        Log an information message
        """
        self.logger.info(f"{self.pipelinejobid} : {msg}")
        self._check_memory_usage()

    def log_error(self, msg: str):
        """
        Log an error message string
        """
        self.logger.error(f"{self.pipelinejobid} : {msg}")
        self._check_memory_usage()

    def log_exception(self, ex: Exception):
        """
        Log an exception
        """
        self.logger.exception(f"{self.pipelinejobid} : {ex}")
        self._check_memory_usage()

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

    def _check_memory_usage(self):
        """
        Monitor memory usage and trigger flush if necessary
        """
        self._log_count += 1
        current_time = time.time()
        
        # Check memory usage periodically
        if (self._log_count % self._LOG_COUNT_THRESHOLD == 0 or 
            current_time - self._last_memory_check > 30):  # Check every 30 seconds
            
            try:
                process = psutil.Process(os.getpid())
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                if memory_mb > self._MAX_LOG_MEMORY_MB:
                    print(f"Memory usage ({memory_mb:.1f}MB) exceeds threshold ({self._MAX_LOG_MEMORY_MB}MB), forcing flush")
                    self.flush_logging(timeout_seconds=30, force=True)
                    
                self._last_memory_check = current_time
                
            except Exception as e:
                # Don't fail logging due to memory check issues
                print(f"Memory check failed: {e}")

    def flush_logging(self, timeout_seconds: int = 60, force: bool = False):
        """
        Attempt to flush logs to Azure App Insights using process isolation
        
        Args:
            timeout_seconds: Maximum time to wait for flush completion
            force: If True, bypass concurrency limits (use with caution)
        """
        if not force and self._flush_in_progress:
            print("Flush already in progress, skipping duplicate flush request")
            return
            
        with self._FLUSH_LOCK:
            if not force and self._CURRENT_FLUSH_COUNT >= self._MAX_FLUSH_PROCESSES:
                print(f"Max flush processes ({self._MAX_FLUSH_PROCESSES}) reached, skipping flush")
                return
                
            self._CURRENT_FLUSH_COUNT += 1
            self._flush_in_progress = True
            
        try:
            print("Starting log flush process")
            
            def flush_target():
                """Target function for flush process"""
                try:
                    print("Process: Flushing logs")
                    # Create a new logger provider instance in the subprocess
                    # to avoid sharing state with the main process
                    self.LOGGER_PROVIDER.force_flush()
                    print("Process: Flush completed successfully")
                except Exception as e:
                    print(f"Process: Flush failed: {e}")
                    
            # Use multiprocessing for better isolation
            process = multiprocessing.Process(target=flush_target)
            process.daemon = True
            process.start()
            
            # Wait for completion with timeout
            process.join(timeout=timeout_seconds)
            
            if process.is_alive():
                print(f"Flush process hung for >{timeout_seconds}s, terminating process")
                process.terminate()
                # Give it a moment to terminate gracefully
                time.sleep(1)
                if process.is_alive():
                    print("Force killing hung flush process")
                    process.kill()
                process.join(timeout=5)
            else:
                print("Log flush completed successfully")
                
        except Exception as e:
            print(f"Error during flush process management: {e}")
        finally:
            with self._FLUSH_LOCK:
                self._CURRENT_FLUSH_COUNT -= 1
                self._flush_in_progress = False
                
    def shutdown_logging(self):
        """
        Shutdown logging provider and clean up resources
        """
        try:
            print("Shutting down logging provider")
            # Final flush before shutdown
            self.flush_logging(timeout_seconds=30, force=True)
            self.LOGGER_PROVIDER.shutdown()
            print("Logging provider shutdown completed")
        except Exception as e:
            print(f"Error during logging shutdown: {e}")

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
