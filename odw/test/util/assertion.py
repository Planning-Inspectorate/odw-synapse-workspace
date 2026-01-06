from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
import json


def assert_dataframes_equal(expected: DataFrame, actual: DataFrame):
    """
    Check that the two dataframes match. Raises an assertion error if there is a mismatch
    """
    schema_mismatch = set(expected.schema).symmetric_difference(set(actual.schema))
    exception_message = ""
    if schema_mismatch:
        expected_schema = json.dumps(json.loads(expected.schema.json()), indent=4)
        actual_schema = json.dumps(json.loads(actual.schema.json()), indent=4)
        exception_message = (
            "Schema mismatch between expected and actual dataframes\n"
            "Expected dataframe schema\n"
            f"{expected_schema}"
            "\nActual dataframe schema\n"
            f"{actual_schema}"
        )
    assert not schema_mismatch, exception_message
    rows_to_show = 20
    # There seems to be a bug that occasionally appears in spark 3.4 that causes a crash when using exceptAll
    # The workaround is to cache before running that command
    expected.cache()
    actual.cache()
    in_expected_but_not_actual = expected.exceptAll(actual)
    in_actual_but_not_expected = actual.exceptAll(expected)
    data_mismatch = not (in_expected_but_not_actual.isEmpty() and in_actual_but_not_expected.isEmpty())
    if data_mismatch:
        missing_data_sample = in_expected_but_not_actual._jdf.showString(rows_to_show, 20, False)
        unexpected_data_sample = in_actual_but_not_expected._jdf.showString(rows_to_show, 20, False)
        exception_message = (
            "Data mismatch between expected and actual dataframe\n"
            "In expected dataframe but not the actual dataframe\n"
            f"{missing_data_sample}"
            "\nIn actual dataframe but not the expected dataframe\n"
            f"{unexpected_data_sample}"
            "\nExpected dataframe\n"
            f"{expected._jdf.showString(rows_to_show, 20, False)}"
            "\Actual dataframe\n"
            f"{actual._jdf.showString(rows_to_show, 20, False)}"
        )
    assert not data_mismatch, exception_message


def assert_etl_result_successful(etl_result: ETLResult):
    if etl_result is None:
        assert False, f"ETL result is expected to be an instance of ETLResult but was None"
    if isinstance(etl_result, ETLSuccessResult):
        return
    exception = etl_result.metadata.exception
    assert False, f"ETL result was not ETLSuccessResult. The below exception was raised\n{exception}"
