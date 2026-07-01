import mock
from pyspark.sql.types import StringType, StructField, StructType, LongType, IntegerType
from odw.core.etl.transformation.curated.entraid_mipins_curation_process import EntraIDMIPINSCurationProcess
from odw.core.etl.etl_result import ETLSuccessResult
from odw.core.etl.metadata_manager import MetadataManager
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
import pytest

pytestmark = pytest.mark.skip(reason="Curated MIPINS EntraID logic not implemented yet")


class TestEntraIDMIPINSCurationProcess(SparkTestCase):
    def test__entraid_mipins_curation_process__load_data(self):
        test_case = "t_emcp_ld"
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
            mock.patch.object(EntraIDMIPINSCurationProcess, "__init__", return_value=None),
            mock.patch.object(EntraIDMIPINSCurationProcess, "HARMONISED_TABLE", entraid_table),
        ):
            inst = EntraIDMIPINSCurationProcess()
            read_data = inst.load_data()
            assert isinstance(read_data, dict) and "horizon_entraid" in read_data
            assert assert_dataframes_equal(expected_read_data, read_data.get("horizon_entraid", None))

    def test__entraid_mipins_curation_process__clean_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entraid_data = spark.createDataFrame(
            (
                {  # Row should be kept
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
                {  # Duplicate of the first row, should be squashed as part of the DISTINCT part of the query
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
                {  # Row should be kept due to null ingestionDate
                    "EmployeeEntraId": 2,
                    "employeeId": "3456",
                    "id": "someguidB",
                    "givenName": "Frodo",
                    "surname": "Baggins",
                    "userPrincipalName": "another@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": None,
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {  # Row should be kept due to null validTo
                    "EmployeeEntraId": 3,
                    "employeeId": "424256",
                    "id": "someguidC",
                    "givenName": "Samwise",
                    "surname": "Gamgee",
                    "userPrincipalName": "email@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {  # Row should be dropped due to an old ingestionDate
                    "EmployeeEntraId": 4,
                    "employeeId": "24242",
                    "id": "someguidE",
                    "givenName": "Tom",
                    "surname": "Bombadil",
                    "userPrincipalName": "email2@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "1899-12-31 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {  # Row should be dropped due to an old validTo
                    "EmployeeEntraId": 5,
                    "employeeId": "53353",
                    "id": "someguidE",
                    "givenName": "Gandalf",
                    "surname": "Grey",
                    "userPrincipalName": "email3@example.gov.uk",
                    "migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": None,
                    "ValidTo": "1899-12-31 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
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
                    "EmployeeEntraId": 1,
                    "ID": "someguidA",
                    "EmployeeID": 12345,
                    "GivenName": "Bilbo",
                    "Surname": "Baggins",
                    "UserPrincipalName": "someemail@example.gov.uk",
                    "Migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {
                    "EmployeeEntraId": 2,
                    "ID": "someguidB",
                    "EmployeeID": 3456,
                    "GivenName": "Frodo",
                    "Surname": "Baggins",
                    "UserPrincipalName": "another@example.gov.uk",
                    "Migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": None,
                    "ValidTo": "2025-02-01 00:00:00",
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
                {
                    "EmployeeEntraId": 3,
                    "ID": "someguidC",
                    "EmployeeID": 424256,
                    "GivenName": "Samwise",
                    "Surname": "Gamgee",
                    "UserPrincipalName": "email@example.gov.uk",
                    "Migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": None,
                    "RowID": "iclehdbielwebibldiewdieiebdeilbe",
                    "IsActive": "N",
                },
            ),
            schema=StructType(
                [
                    StructField("EmployeeEntraId", IntegerType(), True),
                    StructField("ID", StringType(), True),
                    StructField("EmployeeID", IntegerType(), True),
                    StructField("GivenName", StringType(), True),
                    StructField("Surname", StringType(), True),
                    StructField("UserPrincipalName", StringType(), True),
                    StructField("Migrated", StringType(), True),
                    StructField("ODTSourceSystem", StringType(), True),
                    StructField("SourceSystemID", StringType(), True),
                    StructField("IngestionDate", StringType(), True),
                    StructField("ValidTo", StringType(), True),
                    StructField("RowID", StringType(), True),
                    StructField("IsActive", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(EntraIDMIPINSCurationProcess, "__init__", return_value=None):
            inst = EntraIDMIPINSCurationProcess()
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
            mock.patch.object(EntraIDMIPINSCurationProcess, "__init__", return_value=None),
            mock.patch.object(EntraIDMIPINSCurationProcess, "_clean_data", return_value="1"),
        ):
            inst = EntraIDMIPINSCurationProcess()
            actual_output = inst.process(source_data={"horizon_entraid": entraid_data})
            assert isinstance(actual_output, tuple) and len(actual_output) == 2, "Expected process to return a tuple containing two elements"
            actual_write_info, actual_etl_result = actual_output
            assert isinstance(actual_etl_result, ETLSuccessResult)
            assert expected_write_info == actual_write_info

    def test__entraid_curation_process__run(self):
        with mock.patch.object(EntraIDMIPINSCurationProcess, "__init__", return_value=None):
            inst = EntraIDMIPINSCurationProcess()
            with (
                mock.patch.object(EntraIDMIPINSCurationProcess, "load_data", return_value=None),
                mock.patch.object(EntraIDMIPINSCurationProcess, "process", return_value=None),
                mock.patch.object(EntraIDMIPINSCurationProcess, "write_data", return_value=None),
                mock.patch.object(MetadataManager, "__init__", return_value=None),
                mock.patch.object(MetadataManager, "create", return_value=None),
                mock.patch.object(MetadataManager, "update", return_value=None),
            ):
                inst.run()
                EntraIDMIPINSCurationProcess.load_data.assert_called_once()
                EntraIDMIPINSCurationProcess.process.assert_called_once()
                EntraIDMIPINSCurationProcess.write_data.assert_called_once()
