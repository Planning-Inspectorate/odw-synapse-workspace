import hashlib
from datetime import datetime

import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T

from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil


def _std_schema():
    return T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("employeeId", T.StringType(), True),
            T.StructField("givenName", T.StringType(), True),
            T.StructField("surname", T.StringType(), True),
            T.StructField("userPrincipalName", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
        ]
    )


def _hrm_schema():
    return T.StructType(
        [
            T.StructField("EmployeeEntraId", T.LongType(), True),
            T.StructField("employeeId", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
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


def _source_system_schema():
    return T.StructType(
        [
            T.StructField("SourceSystemID", T.StringType(), True),
        ]
    )


def _row_id(id, employee_id, given_name, surname, upn):
    vals = [id, employee_id, given_name, surname, upn]
    concat = "".join(v if v is not None else "." for v in vals)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def _hrm_row(id, employee_id, given_name, surname, upn, *, is_active="Y", employee_entra_id=1, valid_to=None):
    return (
        employee_entra_id,
        employee_id,
        id,
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


def _source_data(spark, std_rows=None, hrm_rows=None, source_system_id="ss1"):
    return {
        "std_data": spark.createDataFrame(std_rows or [], _std_schema()),
        "hrm_data": spark.createDataFrame(hrm_rows or [], _hrm_schema()),
        "source_system": spark.createDataFrame(
            [(source_system_id,)] if source_system_id else [],
            _source_system_schema(),
        ),
    }


class TestEntraIdHarmonisationProcessIntegration(ETLTestCase):
    def test__run__new_record_written_as_active_with_correct_metadata(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = EntraIdHarmonisationProcess(spark)
        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run(orchestration_run_id="t_r_nrwaawcm", orchestration_entity_name="entraid", orchestration_stage_name="harmonise")

        data_to_write = mock_write.call_args[0][0]
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
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user2", "emp2", "NewFirst", "Last2", "user2@test.com", datetime(2024, 2, 1))],
            hrm_rows=[_hrm_row("user2", "emp2", "OldFirst", "Last2", "user2@test.com", is_active="Y", employee_entra_id=1)],
        )
        inst = EntraIdHarmonisationProcess(spark)
        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run(orchestration_run_id="t_r_crcorwidmo", orchestration_entity_name="entraid", orchestration_stage_name="harmonise")

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]

        actual_df = df.select("id", "givenName", "IsActive", "ValidTo").orderBy("IsActive")
        expected_df = spark.createDataFrame(
            [
                ("user2", "OldFirst", "N", "2024-01-31 00:00:00"),
                ("user2", "NewFirst", "Y", None),
            ],
            actual_df.schema,
        )
        assert df.count() == 2
        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__run__unchanged_record_preserved_as_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        id_, emp, fn, sn, upn = "user3", "emp3", "First3", "Last3", "user3@test.com"
        source_data = _source_data(
            spark,
            std_rows=[(id_, emp, fn, sn, upn, datetime(2024, 1, 15))],
            hrm_rows=[_hrm_row(id_, emp, fn, sn, upn, is_active="Y", employee_entra_id=1)],
        )
        inst = EntraIdHarmonisationProcess(spark)
        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run(orchestration_run_id="t_r_urpaa", orchestration_entity_name="entraid", orchestration_stage_name="harmonise")

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__run__historical_inactive_records_carried_forward(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            hrm_rows=[_hrm_row("user4", "emp4", "Old4", "Last4", "user4@test.com", is_active="N", valid_to="2024-01-14")],
        )
        inst = EntraIdHarmonisationProcess(spark)
        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run(orchestration_run_id="t_r_hircf", orchestration_entity_name="entraid", orchestration_stage_name="harmonise")

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["id"] == "user4"
        assert rows[0]["ValidTo"] == "2024-01-14"
        assert result.metadata.insert_count == 1

    def test__run__output_columns_match_hrm_schema(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = EntraIdHarmonisationProcess(spark)
        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run(orchestration_run_id="t_r_ocmrs", orchestration_entity_name="entraid", orchestration_stage_name="harmonise")

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        assert df.columns == [field.name for field in _hrm_schema()]
