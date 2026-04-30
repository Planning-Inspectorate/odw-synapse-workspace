import hashlib
from datetime import datetime

import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T

from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

_STD_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("employeeId", T.StringType(), True),
        T.StructField("givenName", T.StringType(), True),
        T.StructField("surname", T.StringType(), True),
        T.StructField("userPrincipalName", T.StringType(), True),
        T.StructField("IngestionDate", T.TimestampType(), True),
    ]
)

_HRM_SCHEMA = T.StructType(
    [
        T.StructField("EmployeeEntraId", T.LongType(), True),
        T.StructField("id", T.StringType(), True),
        T.StructField("employeeId", T.StringType(), True),
        T.StructField("givenName", T.StringType(), True),
        T.StructField("surname", T.StringType(), True),
        T.StructField("userPrincipalName", T.StringType(), True),
        T.StructField("Migrated", T.StringType(), True),
        T.StructField("ODTSourceSystem", T.StringType(), True),
        T.StructField("SourceSystemID", T.StringType(), True),
        T.StructField("IngestionDate", T.TimestampType(), True),
        T.StructField("ValidTo", T.StringType(), True),
        T.StructField("RowID", T.StringType(), True),
        T.StructField("IsActive", T.StringType(), True),
    ]
)

_SOURCE_SYSTEM_SCHEMA = T.StructType(
    [
        T.StructField("SourceSystemID", T.StringType(), True),
    ]
)


def _row_id(id, employee_id, given_name, surname, upn):
    """Mirrors the Spark md5(concat(coalesce(col, '.'))) computation used by the process."""
    vals = [id, employee_id, given_name, surname, upn]
    concat = "".join(v if v is not None else "." for v in vals)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def _hrm_row(id, employee_id, given_name, surname, upn, *, is_active="Y", employee_entra_id=1, valid_to=None):
    return (
        employee_entra_id,
        id, employee_id, given_name, surname, upn,
        "0", "EntraID", "ss1",
        datetime(2024, 1, 1, 0, 0, 0),
        valid_to,
        _row_id(id, employee_id, given_name, surname, upn),
        is_active,
    )


class TestEntraIdHarmonisationProcessIntegration(ETLTestCase):
    def _run(self, std_df, hrm_df, source_system_df=None):
        spark = PytestSparkSessionUtil().get_spark_session()
        if source_system_df is None:
            source_system_df = spark.createDataFrame([("ss1",)], _SOURCE_SYSTEM_SCHEMA)
        source_data = {"std_data": std_df, "hrm_data": hrm_df, "source_system": source_system_df}

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.entraid_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.entraid_harmonisation_process.LoggingUtil") as mock_proc_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = EntraIdHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        return data_to_write, result

    def test__entraid_harmonisation_process__run__new_record_writes_active_row_with_expected_metadata(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        std_df = spark.createDataFrame(
            [("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
            _STD_SCHEMA,
        )
        hrm_df = spark.createDataFrame([], _HRM_SCHEMA)
        data_to_write, result = self._run(std_df, hrm_df)
        write_config = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]
        rows = write_config["data"].collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["id"] == "user1"
        assert rows[0]["RowID"] is not None and len(rows[0]["RowID"]) == 32
        assert rows[0]["EmployeeEntraId"] is not None
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__entraid_harmonisation_process__run__changed_record_produces_scd2_history(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        std_df = spark.createDataFrame(
            [("user2", "emp2", "NewFirst", "Last2", "user2@test.com", datetime(2024, 2, 1))],
            _STD_SCHEMA,
        )
        hrm_df = spark.createDataFrame(
            [_hrm_row("user2", "emp2", "OldFirst", "Last2", "user2@test.com", is_active="Y", employee_entra_id=1)],
            _HRM_SCHEMA,
        )
        data_to_write, result = self._run(std_df, hrm_df)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        by_active = {row["IsActive"]: row for row in df.collect()}

        assert df.count() == 2
        assert by_active["Y"]["givenName"] == "NewFirst"
        assert by_active["N"]["givenName"] == "OldFirst"
        assert by_active["N"]["ValidTo"] is not None
        assert result.metadata.insert_count == 2

    def test__entraid_harmonisation_process__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        std_df = spark.createDataFrame([], _STD_SCHEMA)
        hrm_df = spark.createDataFrame([], _HRM_SCHEMA)
        data_to_write, result = self._run(std_df, hrm_df)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__entraid_harmonisation_process__run__historical_records_carried_forward(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        std_df = spark.createDataFrame([], _STD_SCHEMA)
        hrm_df = spark.createDataFrame(
            [_hrm_row("user4", "emp4", "Old4", "Last4", "user4@test.com", is_active="N", valid_to="2024-01-14")],
            _HRM_SCHEMA,
        )
        data_to_write, result = self._run(std_df, hrm_df)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["id"] == "user4"
        assert rows[0]["ValidTo"] == "2024-01-14"
        assert result.metadata.insert_count == 1
