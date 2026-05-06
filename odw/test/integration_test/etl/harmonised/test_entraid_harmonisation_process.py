import hashlib
from datetime import datetime

import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T

from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

# Raw standardised schema: matches the actual table columns, including expected_from used by load_data
_STD_RAW_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("employeeId", T.StringType(), True),
        T.StructField("givenName", T.StringType(), True),
        T.StructField("surname", T.StringType(), True),
        T.StructField("userPrincipalName", T.StringType(), True),
        T.StructField("expected_from", T.StringType(), True),
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

_SOURCE_SYSTEM_FULL_SCHEMA = T.StructType(
    [
        T.StructField("SourceSystemID", T.StringType(), True),
        T.StructField("Description", T.StringType(), True),
        T.StructField("IsActive", T.StringType(), True),
    ]
)


def _row_id(id, employee_id, given_name, surname, upn):
    vals = [id, employee_id, given_name, surname, upn]
    concat = "".join(v if v is not None else "." for v in vals)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def _hrm_row(id, employee_id, given_name, surname, upn, *, is_active="Y", employee_entra_id=1, valid_to=None):
    return (
        employee_entra_id,
        id,
        employee_id,
        given_name,
        surname,
        upn,
        "0",
        "EntraID",
        "ss1",
        datetime(2024, 1, 1, 0, 0, 0),
        valid_to,
        _row_id(id, employee_id, given_name, surname, upn),
        is_active,
    )


class TestEntraIdHarmonisationProcessIntegration(ETLTestCase):
    """
    End-to-end integration tests that exercise the full load_data → process → write_data path.
    spark.sql is run against temp views registered from test DataFrames so no real tables are needed.
    write_data is mocked to capture output without touching ADLS.
    """

    def _run(self, std_raw_rows, hrm_rows, source_system_rows=None):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_df = spark.createDataFrame(std_raw_rows or [], _STD_RAW_SCHEMA)
        hrm_df = spark.createDataFrame(hrm_rows or [], _HRM_SCHEMA)
        source_system_rows = source_system_rows or [("ss1", "SAP HR", "Y")]
        source_df = spark.createDataFrame(source_system_rows, _SOURCE_SYSTEM_FULL_SCHEMA)

        std_df.createOrReplaceTempView("test_entraid_std")
        hrm_df.createOrReplaceTempView("test_entraid_hrm")
        source_df.createOrReplaceTempView("test_entraid_source")

        with (
            mock.patch.object(EntraIdHarmonisationProcess, "STD_TABLE", "test_entraid_std"),
            mock.patch.object(EntraIdHarmonisationProcess, "HRM_TABLE", "test_entraid_hrm"),
            mock.patch.object(EntraIdHarmonisationProcess, "SOURCE_SYSTEM_TABLE", "test_entraid_source"),
            mock.patch("odw.core.etl.transformation.harmonised.entraid_harmonisation_process.Util.get_storage_account", return_value="test_storage"),
            mock.patch("odw.core.etl.transformation.harmonised.entraid_harmonisation_process.LoggingUtil"),
            mock.patch("odw.core.etl.etl_process.LoggingUtil"),
        ):
            inst = EntraIdHarmonisationProcess(spark)
            with mock.patch.object(inst, "write_data") as mock_write:
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        return data_to_write, result

    def test__run__new_record_written_as_active_with_correct_metadata(self):
        data_to_write, result = self._run(
            std_raw_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", "2024-01-15")],
            hrm_rows=[],
        )
        rows = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["id"] == "user1"
        assert rows[0]["ODTSourceSystem"] == "EntraID"
        assert rows[0]["SourceSystemID"] == "ss1"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["RowID"] is not None and len(rows[0]["RowID"]) == 32
        assert rows[0]["EmployeeEntraId"] >= 1
        assert data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__run__changed_record_closes_old_row_with_ingestion_date_minus_one(self):
        hrm_row = _hrm_row("user2", "emp2", "OldFirst", "Last2", "user2@test.com", is_active="Y", employee_entra_id=1)
        data_to_write, result = self._run(
            std_raw_rows=[("user2", "emp2", "NewFirst", "Last2", "user2@test.com", "2024-02-01")],
            hrm_rows=[hrm_row],
        )
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        by_active = {row["IsActive"]: row for row in df.collect()}

        assert df.count() == 2
        assert by_active["Y"]["givenName"] == "NewFirst"
        assert by_active["N"]["givenName"] == "OldFirst"
        assert by_active["N"]["ValidTo"] == "2024-01-31 00:00:00"
        assert result.metadata.insert_count == 2

    def test__run__unchanged_record_preserved_as_active(self):
        id_, emp, fn, sn, upn = "user3", "emp3", "First3", "Last3", "user3@test.com"
        hrm_row = _hrm_row(id_, emp, fn, sn, upn, is_active="Y", employee_entra_id=1)
        data_to_write, result = self._run(
            std_raw_rows=[(id_, emp, fn, sn, upn, "2024-01-15")],
            hrm_rows=[hrm_row],
        )
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__run__historical_inactive_records_carried_forward(self):
        hrm_row = _hrm_row("user4", "emp4", "Old4", "Last4", "user4@test.com", is_active="N", valid_to="2024-01-14")
        data_to_write, result = self._run(
            std_raw_rows=[],
            hrm_rows=[hrm_row],
        )
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["id"] == "user4"
        assert rows[0]["ValidTo"] == "2024-01-14"
        assert result.metadata.insert_count == 1
