# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/monitor/azure-monitor-opentelemetry-exporter/samples/logs/sample_log.py
import logging
import functools
import uuid
from notebookutils import mssparkutils
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from multiprocessing import Process
from tenacity import retry, wait_exponential, stop_after_delay, before_sleep


logger = logging.getLogger()
for h in list(logger.handlers):
    if isinstance(h, LoggingHandler):
        logger.removeHandler(h)


LOGGER_PROVIDER = LoggerProvider()
LOGGING_INITIALISED = globals().get("LOGGING_INITIALISED", False)
MAX_WORKER_POOLS = 5
if "CURRENT_WORKER_POOLS_COUNT" not in globals():
    # Prevent overwriting the variable
    CURRENT_WORKER_POOLS_COUNT = 0

pipelinejobid = mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()


def log_info(msg: str):
    logger.info(f"{pipelinejobid} : {msg}")


def log_error(msg: str):
    logger.error(f"{pipelinejobid} : {msg}")


def log_exception(ex: Exception):
    logger.exception(f"{pipelinejobid} : {ex}")


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_delay(20), reraise=True, before_sleep=before_sleep)
def setup_logging(force=False):
    global LOGGING_INITIALISED

    if LOGGING_INITIALISED and not force:
        log_info("Logging already initialized.")
        return

    try:
        key = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        conn_string = key.split(";")[0]
    except Exception as e:
        print("Failed to get connection string:", e)
        return

    set_logger_provider(LOGGER_PROVIDER)
    exporter = AzureMonitorLogExporter.from_connection_string(conn_string)
    LOGGER_PROVIDER.add_log_record_processor(BatchLogRecordProcessor(exporter, schedule_delay_millis=5000))

    if not any(isinstance(h, LoggingHandler) for h in logger.handlers):
        logger.addHandler(LoggingHandler())

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(logging.StreamHandler())

    logger.setLevel(logging.INFO)
    LOGGING_INITIALISED = True
    log_info("Logging initialized.")


def flush_logging(timeout: int = 60):
    global MAX_WORKER_POOLS
    global CURRENT_WORKER_POOLS_COUNT

    def flush_logging_inner():
        print("Flushing logs")
        try:
            LOGGER_PROVIDER.force_flush()
        except Exception as e:
            print(f"Flush failed: {e}")

    if CURRENT_WORKER_POOLS_COUNT < MAX_WORKER_POOLS:
        CURRENT_WORKER_POOLS_COUNT += 1
        t = Process(target=flush_logging_inner, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            print(f"Logging flush timed out in odw/core/util/logging_util.py. Waited {timeout} seconds. Killing the running thread")
            t.terminate()
        CURRENT_WORKER_POOLS_COUNT -= 1
    else:
        print("Could not start logging export in odw/core/util/logging_util.py - max number of flush threads has been reached")


def shutdown_logging():
    try:
        LOGGER_PROVIDER.shutdown()
    except Exception as e:
        print(f"Shutdown failed: {e}")


def logging_to_appins(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        log_info(f"Function {func.__name__} called with args: {', '.join(args_repr + kwargs_repr)}")
        try:
            return func(*args, **kwargs)
        except mssparkutils.handlers.notebookHandler.NotebookExit as e:
            log_info(f"Notebook exited: {e}")
            mssparkutils.notebook.exit(e)
        except Exception as e:
            log_exception(e)
            raise

    return wrapper


if not LOGGING_INITIALISED:
    setup_logging()
    flush_logging()
