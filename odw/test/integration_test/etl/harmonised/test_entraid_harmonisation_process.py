import hashlib
import uuid
from datetime import datetime

import pyspark.sql.types as T

from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil
import odw.test.util.mock.import_mock_notebook_utils


def _std_raw_schema():
    """Schema for odw_standardised_db.entraid — includes expected_from, not IngestionDate."""
    return T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("employeeId", T.StringType(), True),
            T.StructField("givenName", T.StringType(), True),
            T.StructField("surname", T.StringType(), True),
            T.StructField("userPrincipalName", T.StringType(), True),
            T.StructField("expected_from", T.StringType(), True),
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
            T.StructField("SourceSystemId", T.StringType()),
            T.StructField("Description", T.StringType()),
            T.StructField("IngestionDate", T.StringType()),
            T.StructField("ValidTo", T.StringType()),
            T.StructField("RowID", T.StringType()),
            T.StructField("IsActive", T.StringType()),
        ]
    )


def _row_id(id, employee_id, given_name, surname, upn):
    vals = [id, employee_id, given_name, surname, upn]
    concat = "".join(v if v is not None else "." for v in vals)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def _std_row(id, employee_id, given_name, surname, upn, expected_from="2024-01-15 00:00:00"):
    return (id, employee_id, given_name, surname, upn, expected_from)


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


class TestEntraIdHarmonisationProcessIntegration(ETLTestCase):
    def setup_method(self):
        self.test_suffix = uuid.uuid4().hex

    def _write_std(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _std_raw_schema()),
            "entraid",
            "odw_standardised_db",
            "odw-standardised",
            f"entraid/{self.test_suffix}",
            "overwrite",
        )

    def _write_hrm(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _hrm_schema()),
            "entraid",
            "odw_harmonised_db",
            "odw-harmonised",
            "entraid",
            "overwrite",
            {"overwriteSchema": "true"},
        )

    def _write_source_system(self, spark, source_system_id="ss1"):
        df = spark.createDataFrame([(source_system_id, "SAP HR", "", None, "", "Y")], _source_system_schema())
        spark.sql("DROP TABLE IF EXISTS odw_harmonised_db.main_sourcesystem_fact")
        df.write.format("parquet").mode("overwrite").saveAsTable("odw_harmonised_db.main_sourcesystem_fact")

    def test__run__new_record_written_as_active_with_correct_metadata(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_std(spark, [_std_row("user1", "emp1", "Alice", "Smith", "alice@test.com")])
        self._write_hrm(spark)
        self._write_source_system(spark)

        result = EntraIdHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(EntraIdHarmonisationProcess.OUTPUT_TABLE)
        rows = actual.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["id"] == "user1"
        assert rows[0]["ODTSourceSystem"] == "EntraID"
        assert rows[0]["SourceSystemID"] == "ss1"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["RowID"] is not None and len(rows[0]["RowID"]) == 32
        assert rows[0]["EmployeeEntraId"] >= 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__run__changed_record_closes_old_row_with_ingestion_date_minus_one(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_std(spark, [_std_row("user2", "emp2", "NewFirst", "Last2", "user2@test.com", "2024-02-01 00:00:00")])
        self._write_hrm(spark, [_hrm_row("user2", "emp2", "OldFirst", "Last2", "user2@test.com", is_active="Y", employee_entra_id=1)])
        self._write_source_system(spark)

        result = EntraIdHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(EntraIdHarmonisationProcess.OUTPUT_TABLE)
        actual_df = actual.select("id", "givenName", "IsActive", "ValidTo").orderBy("IsActive")

        expected_df = spark.createDataFrame(
            [
                ("user2", "OldFirst", "N", "2024-01-31 00:00:00"),
                ("user2", "NewFirst", "Y", None),
            ],
            actual_df.schema,
        )
        assert actual.count() == 2
        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__run__unchanged_record_preserved_as_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        id_, emp, fn, sn, upn = "user3", "emp3", "First3", "Last3", "user3@test.com"
        self._write_std(spark, [_std_row(id_, emp, fn, sn, upn)])
        self._write_hrm(spark, [_hrm_row(id_, emp, fn, sn, upn, is_active="Y", employee_entra_id=1)])
        self._write_source_system(spark)

        result = EntraIdHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(EntraIdHarmonisationProcess.OUTPUT_TABLE)

        assert actual.count() == 1
        assert actual.collect()[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__run__historical_inactive_records_carried_forward(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_std(spark)
        self._write_hrm(spark, [_hrm_row("user4", "emp4", "Old4", "Last4", "user4@test.com", is_active="N", valid_to="2024-01-14")])
        self._write_source_system(spark)

        result = EntraIdHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(EntraIdHarmonisationProcess.OUTPUT_TABLE)
        rows = actual.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["id"] == "user4"
        assert rows[0]["ValidTo"] == "2024-01-14"
        assert result.metadata.insert_count == 1

    def test__run__output_columns_match_hrm_schema(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_std(spark, [_std_row("user1", "emp1", "Alice", "Smith", "alice@test.com")])
        self._write_hrm(spark)
        self._write_source_system(spark)

        EntraIdHarmonisationProcess(spark).run()

        actual = spark.table(EntraIdHarmonisationProcess.OUTPUT_TABLE)
        assert actual.columns == [field.name for field in _hrm_schema()]
