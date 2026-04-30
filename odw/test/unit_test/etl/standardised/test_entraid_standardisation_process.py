from datetime import datetime

import mock
import pyspark.sql.types as T

from odw.core.etl.transformation.standardised.entraid_standardisation_process import EntraIdStandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

_VALUE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("employeeId", T.StringType(), True),
        T.StructField("givenName", T.StringType(), True),
        T.StructField("surname", T.StringType(), True),
        T.StructField("userPrincipalName", T.StringType(), True),
    ]
)

_RAW_ENTRAID_SCHEMA = T.StructType(
    [
        T.StructField("value", T.ArrayType(_VALUE_SCHEMA), True),
    ]
)


def _raw_df(spark, records):
    return spark.createDataFrame([([dict(r) for r in records],)], _RAW_ENTRAID_SCHEMA)


def _empty_raw_df(spark):
    return spark.createDataFrame([([],)], _RAW_ENTRAID_SCHEMA)


class TestEntraIdStandardisationProcess(SparkTestCase):
    def _run_process(self, raw_df, folder_name="2024-01-15"):
        spark = PytestSparkSessionUtil().get_spark_session()
        with (
            mock.patch(
                "odw.core.etl.transformation.standardised.entraid_standardisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.transformation.standardised.entraid_standardisation_process.LoggingUtil"),
        ):
            inst = EntraIdStandardisationProcess(spark)
            inst._folder_name = folder_name
            return inst.process(source_data={"raw_entraid": raw_df})

    # ------------------------------------------------------------------
    # get_name
    # ------------------------------------------------------------------

    def test__get_name__returns_expected_name(self):
        assert EntraIdStandardisationProcess.get_name() == "EntraID Standardisation"

    # ------------------------------------------------------------------
    # process – row count
    # ------------------------------------------------------------------

    def test__process__explodes_value_array_into_flat_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(
            spark,
            [
                {"id": "u1", "employeeId": "e1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"},
                {"id": "u2", "employeeId": "e2", "givenName": "Bob", "surname": "Jones", "userPrincipalName": "bob@test.com"},
            ],
        )
        data_to_write, _ = self._run_process(raw_df)
        assert data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].count() == 2

    def test__process__empty_value_array_produces_zero_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        data_to_write, result = self._run_process(_empty_raw_df(spark))
        assert data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].count() == 0
        assert result.metadata.insert_count == 0

    # ------------------------------------------------------------------
    # process – schema
    # ------------------------------------------------------------------

    def test__process__output_contains_expected_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(spark, [{"id": "u1", "employeeId": "e1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"}])
        data_to_write, _ = self._run_process(raw_df)
        cols = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].columns
        for col in ("id", "employeeId", "givenName", "surname", "userPrincipalName", "expected_from"):
            assert col in cols, f"Expected column '{col}' in output"

    # ------------------------------------------------------------------
    # process – field values
    # ------------------------------------------------------------------

    def test__process__field_values_are_correctly_extracted(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(spark, [{"id": "u1", "employeeId": "emp1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"}])
        data_to_write, _ = self._run_process(raw_df)
        row = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["id"] == "u1"
        assert row["employeeId"] == "emp1"
        assert row["givenName"] == "Alice"
        assert row["surname"] == "Smith"
        assert row["userPrincipalName"] == "alice@test.com"

    def test__process__null_optional_fields_are_preserved(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(spark, [{"id": "u1", "employeeId": None, "givenName": "Alice", "surname": None, "userPrincipalName": "alice@test.com"}])
        data_to_write, _ = self._run_process(raw_df)
        row = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["employeeId"] is None
        assert row["surname"] is None

    # ------------------------------------------------------------------
    # process – expected_from
    # ------------------------------------------------------------------

    def test__process__expected_from_is_day_before_folder_date(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(spark, [{"id": "u1", "employeeId": "e1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"}])
        data_to_write, _ = self._run_process(raw_df, folder_name="2024-01-15")
        row = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["expected_from"] == datetime(2024, 1, 14, 0, 0, 0)

    def test__process__all_rows_share_the_same_expected_from(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(
            spark,
            [
                {"id": "u1", "employeeId": "e1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"},
                {"id": "u2", "employeeId": "e2", "givenName": "Bob", "surname": "Jones", "userPrincipalName": "bob@test.com"},
            ],
        )
        data_to_write, _ = self._run_process(raw_df, folder_name="2024-03-10")
        rows = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].collect()
        expected = datetime(2024, 3, 9, 0, 0, 0)
        assert all(row["expected_from"] == expected for row in rows)

    # ------------------------------------------------------------------
    # process – write config
    # ------------------------------------------------------------------

    def test__process__write_config_has_overwrite_mode_and_delta_format(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(spark, [{"id": "u1", "employeeId": "e1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"}])
        data_to_write, result = self._run_process(raw_df)
        write_config = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1

    # ------------------------------------------------------------------
    # ETLProcessFactory registration
    # ------------------------------------------------------------------

    def test__entraid_standardisation_process__is_registered_in_factory(self):
        from odw.core.etl.etl_process_factory import ETLProcessFactory

        process_class = ETLProcessFactory.get("EntraID Standardisation")
        assert process_class is EntraIdStandardisationProcess
