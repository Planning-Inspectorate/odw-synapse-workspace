from odw.test.util.test_case import TestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from filelock import FileLock
from typing import List, Type
import time
import os
import logging
import pytest
import sys
import inspect
import importlib


"""
This module defines functions that should be used by `conftest.py` files
"""


def configure_session():
    """
    This is run at the start of every pytest session. It will be called by each worker thread
    """
    PytestSparkSessionUtil()
    import_all_testing_modules()


def import_all_testing_modules():
    """
    Import all test modules
    """
    # Extract all testing modules under the `test/` directory
    module_content_to_exclude = {"__init__.py", "__pycache__", "conftest.py"}
    python_modules_to_load = []
    files_to_explore = [x for x in os.listdir(os.path.join("odw", "test")) if "test" in x and os.path.isdir(os.path.join("odw", "test", x))]
    while files_to_explore:
        next_file = files_to_explore.pop(0)
        if os.path.isfile(os.path.join("odw", "test", next_file)):
            if next_file.endswith(".py") and all(x not in next_file for x in module_content_to_exclude):
                python_modules_to_load.append(next_file)
        else:
            files_to_explore.extend([os.path.join(next_file, x) for x in os.listdir(os.path.join("odw", "test", next_file))])
    python_modules_to_load_cleaned = sorted([x.replace(".py", "").replace(os.sep, ".") for x in python_modules_to_load])
    python_modules_to_load_cleaned = [f"odw.test.{x}" if not x.startswith("odw.test") else x for x in python_modules_to_load_cleaned]
    # Import all testing modules
    for module_to_import in python_modules_to_load_cleaned:
        importlib.import_module(module_to_import)
    return python_modules_to_load_cleaned


def extract_all_test_cases():
    """
    Iterate through the imported modules to select all TestCase instances
    """
    test_modules = [module for module in sys.modules if module.startswith("odw.test.")]
    return {
        obj
        for module in test_modules
        for name, obj in inspect.getmembers(sys.modules[module])
        if inspect.isclass(obj) and issubclass(obj, TestCase) and not obj == TestCase
    }


def process_arguments(session) -> List[Type[TestCase]]:
    """
    Process the pytest invocation parameters to return a list of test cases whos
    module setup/teardown functions need to be called
    """
    pytest_args = session.config.invocation_params.args

    # If we're doing a marker-only run (e.g. -m e2e), do NOT run the unit/integration
    # TestCase session_setup/teardown for the whole suite.
    if "-m" in pytest_args:
        m_idx = pytest_args.index("-m")
        if m_idx + 1 < len(pytest_args) and "e2e" in pytest_args[m_idx + 1]:
            return []

    test_cases = extract_all_test_cases()
    directory_args = [x.replace(".py", "") for x in pytest_args if x.startswith("test")]
    # If no arguments were given or if no specific python files were given then
    # all tests are being executed
    if not directory_args:
        return test_cases
    test_case_module_map = {test_case.__module__.replace(".", "/"): test_case for test_case in test_cases}
    matched_modules = []
    for directory in directory_args:
        matches = [module for module in test_case_module_map.keys() if directory in module]
        matched_modules += matches
    return [test_case_module_map[directory] for directory in set(matched_modules)]


def _session_setup_task(session):
    """
    Initialise the main thread. This sets up reusable items that must be used across all workers
    """
    logging.info("Setting up pytest session for tests")
    for test_case in process_arguments(session):
        logging.info("    Running setup for " + test_case.__module__)
        test_case().session_setup()


@pytest.fixture(scope="session", autouse=True)
def session_setup(tmp_path_factory, worker_id, request):
    """
    Initialise the main and worker threads. This ensures `_session_setup_task` is called only by the main thread
    """
    # Code based on example from docs at
    # https://pytest-xdist.readthedocs.io/en/latest/how-to.html#making-session-scoped-fixtures-execute-only-once
    if worker_id == "master":
        return _session_setup_task(request.session)
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "setupsession.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            master_worker_written_value = fn.read_text()
            if "Failed" in master_worker_written_value:
                pytest.fail("Master thread failed during setup")
        else:
            fn.write_text("Starting session setup")
            try:
                _session_setup_task(request.session)
                fn.write_text("Complete")
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                fn.write_text("Failed")
                raise e


def _session_teardown_task(session):
    """
    Teardown the main thread. This will be called as the last thing that happens in the test
    """
    logging.info("Tearing down pytest session for tests")
    for test_case in process_arguments(session):
        logging.info("    Running teardown for " + test_case.__module__)
        t0 = time.perf_counter()
        test_case().session_teardown()
        logging.info(
            "    Teardown complete for %s (%.2fs)",
            test_case.__module__,
            time.perf_counter() - t0,
        )
    PytestSparkSessionUtil().teardown_all_file_systems()

    # Best-effort: stop OTel providers if they exist
    try:
        from opentelemetry import trace, metrics

        tp = trace.get_tracer_provider()
        mp = metrics.get_meter_provider()

        shutdown = getattr(tp, "shutdown", None)
        if callable(shutdown):
            shutdown()

        shutdown = getattr(mp, "shutdown", None)
        if callable(shutdown):
            shutdown()
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def session_teardown(tmp_path_factory, worker_id, request):
    """
    Teardown the main and worker threads. This ensures `_session_teardown_task` is called only by the main thread
    """
    # Code based on example from docs at
    # https://pytest-xdist.readthedocs.io/en/latest/how-to.html#making-session-scoped-fixtures-execute-only-once
    yield
    if worker_id == "master":
        return _session_teardown_task(request.session)
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "teardownsession.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            number_of_completed_workers = int(fn.read_text()) + 1
            fn.write_text(str(number_of_completed_workers))
        else:
            number_of_completed_workers = 1
            fn.write_text(str(number_of_completed_workers))
    # Call teardown if this call was initiated by the last worker that is still running
    num_tests = len(request.session.items)
    if num_tests < request.config.workerinput["workercount"]:
        last_worker = number_of_completed_workers >= num_tests
    else:
        last_worker = number_of_completed_workers >= request.config.workerinput["workercount"]
    logging.info("Num tests: " + str(num_tests))
    logging.info("completed workers count: " + str(number_of_completed_workers))
    logging.info("Last worker: " + str(last_worker))
    if last_worker:
        _session_teardown_task(request.session)
