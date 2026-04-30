from datetime import datetime

import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T

from odw.core.etl.transformation.standardised.entraid_standardisation_process import EntraIdStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

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


class TestEntraIdStandardisationProcessIntegration(ETLTestCase):
    def _run(self, raw_df, folder_name="2024-01-15"):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = {"raw_entraid": raw_df}

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.standardised.entraid_standardisation_process.LoggingUtil") as mock_proc_logging,
            mock.patch(
                "odw.core.etl.transformation.standardised.entraid_standardisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = EntraIdStandardisationProcess(spark)
            inst._folder_name = folder_name

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        return data_to_write, result

    def test__entraid_standardisation_process__run__single_record_writes_expected_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(
            spark,
            [{"id": "u1", "employeeId": "emp1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"}],
        )
        data_to_write, result = self._run(raw_df, folder_name="2024-01-15")
        write_config = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]
        rows = write_config["data"].collect()

        assert len(rows) == 1
        assert rows[0]["id"] == "u1"
        assert rows[0]["givenName"] == "Alice"
        assert rows[0]["expected_from"] == datetime(2024, 1, 14, 0, 0, 0)
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__entraid_standardisation_process__run__multiple_records_all_written(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(
            spark,
            [
                {"id": "u1", "employeeId": "emp1", "givenName": "Alice", "surname": "Smith", "userPrincipalName": "alice@test.com"},
                {"id": "u2", "employeeId": "emp2", "givenName": "Bob", "surname": "Jones", "userPrincipalName": "bob@test.com"},
                {"id": "u3", "employeeId": "emp3", "givenName": "Carol", "surname": "White", "userPrincipalName": "carol@test.com"},
            ],
        )
        data_to_write, result = self._run(raw_df)
        df = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 3
        assert result.metadata.insert_count == 3

    def test__entraid_standardisation_process__run__empty_value_array_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = spark.createDataFrame([([],)], _RAW_ENTRAID_SCHEMA)
        data_to_write, result = self._run(raw_df)
        df = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 0
        assert data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__entraid_standardisation_process__run__null_optional_fields_preserved(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_df(
            spark,
            [{"id": "u1", "employeeId": None, "givenName": "Alice", "surname": None, "userPrincipalName": "alice@test.com"}],
        )
        data_to_write, _ = self._run(raw_df)
        row = data_to_write[EntraIdStandardisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["employeeId"] is None
        assert row["surname"] is None
