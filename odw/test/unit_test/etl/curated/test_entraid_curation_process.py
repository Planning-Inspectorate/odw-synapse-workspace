import mock
from pyspark.sql.types import StringType, StructField, StructType, LongType
from odw.core.etl.transformation.curated.entraid_curation_process import EntraIDCurationProcess
from odw.core.etl.etl_result import ETLSuccessResult
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
import pytest

pytestmark = pytest.mark.xfail(reason="Curated EntraID logic not implemented yet")


class TestEntraIDCurationProcess(SparkTestCase):
    def test__entraid_curation_process__load_data(self):
        test_case = "t_ecp_ld"
        spark = PytestSparkSessionUtil().get_spark_session()
        entraid_data = spark.createDataFrame(
            (
                {
                    "EmployeeEntraId": 1,
                    "employeeId": "12345",
                    "id": "someguidA",
                    "givenName": "Bilbo",
                    "surname": "Baggins",
                    "userPrincipalName": "someemail@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {
                    "EmployeeEntraId": 2,
                    "employeeId": "678910",
                    "id": "someguidB",
                    "givenName": "Sheev",
                    "surname": "Palpatine",
                    "userPrincipalName": "anotheremail@example.gov.uk",
                    "migrated": "1",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "ucewcybucewebheblwieiblwwhbj",
                    "IsActive": "Y",
                },
            ),
            schema=StructType(
                [
                    StructField("EmployeeEntraId", LongType(), True),
                    StructField("employeeId", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("givenName", StringType(), True),
                    StructField("surname", StringType(), True),
                    StructField("userPrincipalName", StringType(), True),
                    StructField("migrated", StringType(), True),
                    StructField("ODTSourceSystem", StringType(), True),
                    StructField("SourceSystemID", StringType(), True),
                    StructField("IngestionDate", StringType(), True),
                    StructField("ValidTo", StringType(), True),
                    StructField("RowID", StringType(), True),
                    StructField("IsActive", StringType(), True),
                ]
            ),
        )
        entraid_table = f"{test_case}_entraid"
        self.write_existing_table(spark, entraid_data, entraid_table, "odw_harmonised_db", "odw-harmonised", entraid_table, "overwrite")
        expected_read_data = entraid_data
        with (
            mock.patch.object(EntraIDCurationProcess, "__init__", return_value=None),
            mock.patch.object(EntraIDCurationProcess, "HARMONISED_TABLE", entraid_table),
        ):
            inst = EntraIDCurationProcess()
            read_data = inst.load_data()
            assert isinstance(read_data, dict) and "horizon_entraid" in read_data
            assert assert_dataframes_equal(expected_read_data, read_data.get("horizon_entraid", None))

    def test__entraid_curation_process__clean_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entraid_data = spark.createDataFrame(
            (
                {
                    "EmployeeEntraId": 1,
                    "employeeId": "12345",
                    "id": "someguidA",
                    "givenName": "Bilbo",
                    "surname": "Baggins",
                    "userPrincipalName": "someemail@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",  # Should be dropped due to this being set to 'N'
                },
                {
                    "EmployeeEntraId": 2,
                    "employeeId": "678910",
                    "id": "someguidB",
                    "givenName": "Sheev",
                    "surname": "Palpatine",
                    "userPrincipalName": "anotheremail@example.gov.uk",
                    "migrated": "1",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "ucewcybucewebheblwieiblwwhbj",
                    "IsActive": "Y",
                },
                {  # Duplicate of the 2nd row for the relavnt columns
                    "EmployeeEntraId": 3,
                    "employeeId": "678910",
                    "id": "someguidB",
                    "givenName": "Sheev",
                    "surname": "Palpatine",
                    "userPrincipalName": "anotheremail@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2023-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "hejdkbwdjhkbdkehjjdkewhd",
                    "IsActive": "Y",
                },
                {
                    "EmployeeEntraId": 4,
                    "employeeId": "115935",
                    "id": "someguidC",
                    "givenName": "Frodo",
                    "surname": "Baggins",
                    "userPrincipalName": "email@example.gov.uk",
                    "migrated": "1",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2026-01-01 00:00:00",
                    "ValidTo": "2026-02-01 00:00:00",
                    "RowID": "duvyvuy3dudy3id",
                    "IsActive": "Y",
                },
            ),
            schema=StructType(
                [
                    StructField("EmployeeEntraId", LongType(), True),
                    StructField("employeeId", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("givenName", StringType(), True),
                    StructField("surname", StringType(), True),
                    StructField("userPrincipalName", StringType(), True),
                    StructField("migrated", StringType(), True),
                    StructField("ODTSourceSystem", StringType(), True),
                    StructField("SourceSystemID", StringType(), True),
                    StructField("IngestionDate", StringType(), True),
                    StructField("ValidTo", StringType(), True),
                    StructField("RowID", StringType(), True),
                    StructField("IsActive", StringType(), True),
                ]
            ),
        )

        expected_clean_data = spark.createDataFrame(
            (
                {
                    "employeeId": "678910",
                    "id": "someguidB",
                    "givenName": "Sheev",
                    "surname": "Palpatine",
                    "userPrincipalName": "anotheremail@example.gov.uk",
                },
                {"employeeId": "115935", "id": "someguidC", "givenName": "Frodo", "surname": "Baggins", "userPrincipalName": "email@example.gov.uk"},
            ),
            schema=StructType(
                [
                    StructField("employeeId", StringType(), False),
                    StructField("id", StringType(), True),
                    StructField("givenName", StringType(), False),
                    StructField("surname", StringType(), False),
                    StructField("userPrincipalName", StringType(), False),
                ]
            ),
        )
        with mock.patch.object(EntraIDCurationProcess, "__init__", return_value=None):
            inst = EntraIDCurationProcess()
            actual_clean_data = inst._clean_data(entraid_data)
            assert_dataframes_equal(expected_clean_data, actual_clean_data)

    def test__entraid_curation_process__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entraid_data = spark.createDataFrame(
            (
                {
                    "EmployeeEntraId": 1,
                    "employeeId": "12345",
                    "id": "someguidA",
                    "givenName": "Bilbo",
                    "surname": "Baggins",
                    "userPrincipalName": "someemail@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {
                    "EmployeeEntraId": 2,
                    "employeeId": "678910",
                    "id": "someguidB",
                    "givenName": "Sheev",
                    "surname": "Palpatine",
                    "userPrincipalName": "anotheremail@example.gov.uk",
                    "migrated": "1",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "ucewcybucewebheblwieiblwwhbj",
                    "IsActive": "Y",
                },
            ),
            schema=StructType(
                [
                    StructField("EmployeeEntraId", LongType(), True),
                    StructField("employeeId", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("givenName", StringType(), True),
                    StructField("surname", StringType(), True),
                    StructField("userPrincipalName", StringType(), True),
                    StructField("migrated", StringType(), True),
                    StructField("ODTSourceSystem", StringType(), True),
                    StructField("SourceSystemID", StringType(), True),
                    StructField("IngestionDate", StringType(), True),
                    StructField("ValidTo", StringType(), True),
                    StructField("RowID", StringType(), True),
                    StructField("IsActive", StringType(), True),
                ]
            ),
        )
        expected_write_info = {}
        with (
            mock.patch.object(EntraIDCurationProcess, "__init__", return_value=None),
            mock.patch.object(EntraIDCurationProcess, "_clean_data", return_value="1"),
        ):
            inst = EntraIDCurationProcess()
            actual_output = inst.process(source_data={"horizon_entraid": entraid_data})
            assert isinstance(actual_output, tuple) and len(actual_output) == 2, "Expected process to return a tuple containing two elements"
            actual_write_info, actual_etl_result = actual_output
            assert isinstance(actual_etl_result, ETLSuccessResult)
            assert expected_write_info == actual_write_info

    def test__entraid_curation_process__run(self):
        with mock.patch.object(EntraIDCurationProcess, "__init__", return_value=None):
            inst = EntraIDCurationProcess()
            with (
                mock.patch.object(EntraIDCurationProcess, "load_data", return_value=None),
                mock.patch.object(EntraIDCurationProcess, "process", return_value=None),
                mock.patch.object(EntraIDCurationProcess, "write_data", return_value=None),
            ):
                inst.run()
                EntraIDCurationProcess.load_data.assert_called_once()
                EntraIDCurationProcess.process.assert_called_once()
                EntraIDCurationProcess.write_data.assert_called_once()
