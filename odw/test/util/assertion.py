from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.test.util.config import TEST_CONFIG
from pyspark.sql import DataFrame
import json
import inspect
import os


"""
This module contains various assertions that are useful for testing
"""


def _get_test_function_caller():
    for x in inspect.stack():
        function = x.function
        if function.startswith("test") or function.endswith("test"):
            return function


def assert_dataframes_equal(expected: DataFrame, actual: DataFrame):
    """
    Check that the two dataframes match. Raises an assertion error if there is a mismatch
    """
    if expected is None and actual is None:
        # Both are none, so the data matches
        return
    assert isinstance(expected, DataFrame), (
        f"Expected expected to be a dataframe, but was of type {type(expected)}"
    )
    assert isinstance(actual, DataFrame), (
        f"Expected actual to be a dataframe, but was of type {type(actual)}"
    )
    save_local_data = TEST_CONFIG.get("DUMP_ASSERTION_DATA", False)
    caller = _get_test_function_caller()
    if save_local_data:
        expected.coalesce(1).write.mode("overwrite").json(
            os.path.join("testOutput", f"{caller}_expected")
        )
        actual.coalesce(1).write.mode("overwrite").json(
            os.path.join("testOutput", f"{caller}_actual")
        )
    schema_mismatch = set(expected.schema).symmetric_difference(set(actual.schema))
    exception_message = ""
    if schema_mismatch:
        expected_schema = json.dumps(json.loads(expected.schema.json()), indent=4)
        actual_schema = json.dumps(json.loads(actual.schema.json()), indent=4)
        extra_in_expected = schema_mismatch.difference(set(actual.schema))
        extra_in_actual = schema_mismatch.difference(set(expected.schema))
        exception_message = (
            "Schema mismatch between expected and actual dataframes\n"
            "Expected dataframe schema\n"
            f"{expected_schema}"
            "\nActual dataframe schema\n"
            f"{actual_schema}\n"
            f"Columns in expected but not actual: {extra_in_expected}\n"
            f"Columns in actual but not expected: {extra_in_actual}\n"
        )
    assert not schema_mismatch, exception_message
    rows_to_show = 20
    # There seems to be a bug that occasionally appears in spark 3.4 that causes a crash when using exceptAll
    # The workaround is to cache before running that command
    expected_cached = expected.cache()
    actual_cached = actual.cache()

    try:
        # Update column order, since column order matters when comparing the data.
        # At this stage both dataframes are guaranteed to have the same schema.
        expected_ordered = expected_cached.select(expected_cached.schema.names)
        actual_ordered = actual_cached.select(expected_cached.schema.names)

        in_expected_but_not_actual = expected_ordered.exceptAll(actual_ordered)
        in_actual_but_not_expected = actual_ordered.exceptAll(expected_ordered)

        data_mismatch = not (
            in_expected_but_not_actual.isEmpty()
            and in_actual_but_not_expected.isEmpty()
        )

        if data_mismatch:
            missing_data_sample = in_expected_but_not_actual._jdf.showString(
                rows_to_show, 20, False
            )
            unexpected_data_sample = in_actual_but_not_expected._jdf.showString(
                rows_to_show, 20, False
            )
            exception_message = (
                "Data mismatch between expected and actual dataframe\n"
                "In expected dataframe but not the actual dataframe\n"
                f"{missing_data_sample}"
                "\nIn actual dataframe but not the expected dataframe\n"
                f"{unexpected_data_sample}"
                "\nExpected dataframe\n"
                f"{expected_ordered._jdf.showString(rows_to_show, 20, False)}"
                "\nActual dataframe\n"
                f"{actual_ordered._jdf.showString(rows_to_show, 20, False)}"
            )

        assert not data_mismatch, exception_message
        if save_local_data:
            in_expected_but_not_actual.coalesce(1).write.mode("overwrite").json(
                os.path.join("testOutput", f"{caller}_in_expected_but_not_actual")
            )
            in_actual_but_not_expected.coalesce(1).write.mode("overwrite").json(
                os.path.join("testOutput", f"{caller}_in_actual_but_not_expected")
            )
    finally:
        expected_cached.unpersist(blocking=True)
        actual_cached.unpersist(blocking=True)


def assert_etl_result_successful(etl_result: ETLResult):
    if etl_result is None:
        assert False, (
            "ETL result is expected to be an instance of ETLResult but was None"
        )
    if isinstance(etl_result, ETLSuccessResult):
        return
    exception_trace = etl_result.metadata.exception_trace
    assert False, (
        f"ETL result was not ETLSuccessResult. The below exception was raised\n{exception_trace}"
    )
