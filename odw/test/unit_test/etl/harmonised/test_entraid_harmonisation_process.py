import hashlib
from datetime import datetime

import mock
import pyspark.sql.types as T

from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


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
    """Mirrors the Spark md5(concat(coalesce(col, '.'))) computation used by the process."""
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


def _process_under_test(spark):
    return EntraIdHarmonisationProcess(spark)


class TestEntraIdHarmonisationProcess(SparkTestCase):
    # ------------------------------------------------------------------
    # get_name
    # ------------------------------------------------------------------

    def test__get_name__returns_expected_name(self):
        assert EntraIdHarmonisationProcess.get_name() == "EntraID Harmonisation"

    # ------------------------------------------------------------------
    # process – new records
    # ------------------------------------------------------------------

    def test__process__new_record_inserted_as_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        rows = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["id"] == "user1"
        assert rows[0]["ODTSourceSystem"] == "EntraID"

    def test__process__new_record_has_populated_row_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        row = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["RowID"] is not None
        assert len(row["RowID"]) == 32

    def test__process__new_record_has_employee_entra_id_assigned(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        row = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()[0]
        assert row["EmployeeEntraId"] is not None
        assert row["EmployeeEntraId"] >= 1

    # ------------------------------------------------------------------
    # process – changed records (SCD2)
    # ------------------------------------------------------------------

    def test__process__changed_record_closes_old_version_and_inserts_new(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user2", "emp2", "NewFirst", "Last2", "user2@test.com", datetime(2024, 2, 1))],
            hrm_rows=[_hrm_row("user2", "emp2", "OldFirst", "Last2", "user2@test.com", is_active="Y", employee_entra_id=1)],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
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

    # ------------------------------------------------------------------
    # process – unchanged records
    # ------------------------------------------------------------------

    def test__process__unchanged_record_preserved_as_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        id_, emp, fn, sn, upn = "user3", "emp3", "First3", "Last3", "user3@test.com"
        source_data = _source_data(
            spark,
            std_rows=[(id_, emp, fn, sn, upn, datetime(2024, 1, 15))],
            hrm_rows=[_hrm_row(id_, emp, fn, sn, upn, is_active="Y", employee_entra_id=1)],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 1
        assert df.collect()[0]["IsActive"] == "Y"

    # ------------------------------------------------------------------
    # process – historical records
    # ------------------------------------------------------------------

    def test__process__historical_inactive_records_preserved(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            hrm_rows=[_hrm_row("user4", "emp4", "Old4", "Last4", "user4@test.com", is_active="N", valid_to="2024-01-14")],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        rows = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        assert len(rows) == 1
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["id"] == "user4"

    # ------------------------------------------------------------------
    # process – EmployeeEntraId
    # ------------------------------------------------------------------

    def test__process__employee_entra_ids_are_unique_and_sequential(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[
                ("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15)),
                ("user2", "emp2", "Bob", "Jones", "bob@test.com", datetime(2024, 1, 15)),
            ],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        ids = sorted([row["EmployeeEntraId"] for row in df.collect()])
        assert ids == [1, 2]

    # ------------------------------------------------------------------
    # process – SourceSystemID
    # ------------------------------------------------------------------

    def test__process__source_system_id_propagated_to_new_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
            source_system_id="sys42",
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        assert data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()[0]["SourceSystemID"] == "sys42"

    def test__process__missing_source_system_sets_none(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
            source_system_id=None,
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        assert data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"].collect()[0]["SourceSystemID"] is None

    # ------------------------------------------------------------------
    # process – write config and output column order
    # ------------------------------------------------------------------

    def test__process__write_config_is_correct(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=source_data)
        write_config = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1

    def test__process__output_columns_match_hrm_schema(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            std_rows=[("user1", "emp1", "Alice", "Smith", "alice@test.com", datetime(2024, 1, 15))],
        )
        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=source_data)
        df = data_to_write[EntraIdHarmonisationProcess.OUTPUT_TABLE]["data"]
        assert df.columns == [field.name for field in _hrm_schema()]

    # ------------------------------------------------------------------
    # ETLProcessFactory registration
    # ------------------------------------------------------------------

    def test__entraid_harmonisation_process__is_registered_in_factory(self):
        from odw.core.etl.etl_process_factory import ETLProcessFactory

        process_class = ETLProcessFactory.get("EntraID Harmonisation")
        assert process_class is EntraIdHarmonisationProcess
