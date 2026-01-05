from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.etl_result import ETLSuccessResult, ETLFailResult, ETLResult
from datetime import datetime
from pyspark.sql import DataFrame
from typing import Dict, Tuple
import mock
import json


class ETLProcessImpl(ETLProcess):
    """
    A blank implementation class for the ETLProcess, which will be used for testing
    """

    @classmethod
    def get_name(cls) -> str:
        return ""

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        pass


def generate_etl_success_result(start_time: datetime = datetime.now(), end_time: datetime = datetime.now()):
    return ETLSuccessResult(
        metadata=ETLResult.ETLResultMetadata(
            start_execution_time=start_time,
            end_execution_time=end_time,
            table_name=None,
            insert_count=0,
            update_count=0,
            delete_count=0,
            activity_type="ETLProcessImpl",
            duration_seconds=(end_time - start_time).total_seconds(),
        )
    )


def generate_etl_fail_result(
    exception: str,
    start_time: datetime = datetime.now(),
    end_time: datetime = datetime.now(),
    table_name: str = None
):
    return ETLFailResult(
        metadata=ETLResult.ETLResultMetadata(
            start_execution_time=start_time,
            end_execution_time=end_time,
            exception=exception,
            exception_trace=exception,
            table_name=table_name,
            insert_count=0,
            update_count=0,
            delete_count=0,
            activity_type="ETLProcessImpl",
            duration_seconds=(end_time - start_time).total_seconds(),
        )
    )


def compare_etl_results(expected: ETLResult, actual: ETLResult):
    actual_result_json = json.loads(actual.model_dump_json())
    expected_result_json = json.loads(expected.model_dump_json())
    actual_error_message = actual_result_json["metadata"].pop("exception_trace", None)
    expected_error_message = expected_result_json["metadata"].pop("exception_trace", None)
    metadata_fields_to_ignore = ("start_execution_time", "end_execution_time", "duration_seconds")
    for field in metadata_fields_to_ignore:
        actual_result_json["metadata"].pop(field)
        expected_result_json["metadata"].pop(field)
    assert expected_result_json == actual_result_json
    assert expected_error_message in actual_error_message


def test__etl_process__run__successful():
    """
    - Given I the read, transformation and writing functions are successful
    - When I call ETLProcess.run
    - Then the ETLResult of the transformation process should be returned
    """
    mock_result = generate_etl_success_result()
    with mock.patch.object(ETLProcessImpl, "__init__", return_value=None):
        with mock.patch.object(ETLProcessImpl, "load_data", return_value=dict()):
            with mock.patch.object(ETLProcessImpl, "process", return_value=(dict(), mock_result)):
                with mock.patch.object(ETLProcessImpl, "write_data"):
                    actual_result = ETLProcessImpl().run(a=None)
                    assert mock_result == actual_result


def test__etl_process__run__read_exception():
    """
    - Given the read function fails with an exception
    - When I call ETLProcess.run
    - Then the exception should be returned as part of ETLFailResult
    """
    mock_result = generate_etl_success_result()
    expected_result = generate_etl_fail_result("Some read exception")
    with mock.patch.object(ETLProcessImpl, "__init__", return_value=None):
        with mock.patch.object(ETLProcessImpl, "load_data", side_effect=Exception("Some read exception")):
            with mock.patch.object(ETLProcessImpl, "process", return_value=(dict(), mock_result)):
                with mock.patch.object(ETLProcessImpl, "write_data"):
                    actual_result = ETLProcessImpl().run(a=None)
                    compare_etl_results(expected_result, actual_result)


def test__etl_process__run__transformation_exception():
    """
    - Given the transformation function fails with an exception
    - When I call ETLProcess.run
    - Then the exception should be returned as part of ETLFailResult
    """
    expected_result = generate_etl_fail_result("Some transformation exception")
    with mock.patch.object(ETLProcessImpl, "__init__", return_value=None):
        with mock.patch.object(ETLProcessImpl, "load_data", return_value=dict()):
            with mock.patch.object(ETLProcessImpl, "process", side_effect=Exception("Some transformation exception")):
                with mock.patch.object(ETLProcessImpl, "write_data"):
                    actual_result = ETLProcessImpl().run(a=None)
                    compare_etl_results(expected_result, actual_result)


def test__etl_process__run__transformation_failure():
    """
    - Given the transformation function fails
    - When I call ETLProcess.run
    - Then the exception should be returned as part of ETLFailResult
    """
    mock_result = generate_etl_fail_result("some transformation exception")
    with mock.patch.object(ETLProcessImpl, "__init__", return_value=None):
        with mock.patch.object(ETLProcessImpl, "load_data", return_value=dict()):
            with mock.patch.object(ETLProcessImpl, "process", return_value=(dict(), mock_result)):
                with mock.patch.object(ETLProcessImpl, "write_data"):
                    actual_result = ETLProcessImpl().run(a=None)
                    compare_etl_results(mock_result, actual_result)


def test__etl_process__run__write_exception():
    """
    - Given the write function fails with an exception
    - When I call ETLProcess.run
    - Then the exception should be returned as part of ETLFailResult
    """
    mock_result = generate_etl_success_result()
    expected_result = generate_etl_fail_result("Some write exception", table_name="table_a, table_b")
    with mock.patch.object(ETLProcessImpl, "__init__", return_value=None):
        with mock.patch.object(ETLProcessImpl, "load_data", return_value=dict()):
            with mock.patch.object(ETLProcessImpl, "process", return_value=({"table_a": None, "table_b": None}, mock_result)):
                with mock.patch.object(ETLProcessImpl, "write_data", side_effect=Exception("Some write exception")):
                    actual_result = ETLProcessImpl().run(a=None)
                    compare_etl_results(expected_result, actual_result)
