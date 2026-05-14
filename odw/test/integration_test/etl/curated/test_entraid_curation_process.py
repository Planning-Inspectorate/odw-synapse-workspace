import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.entraid_curation_process import EntraIDCurationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType
import mock


pytestmark = pytest.mark.xfail(reason="Curated EntraID logic not implemented yet")


class TestEntraIDCurationProcess(ETLTestCase):
    def assert_curation(self, test_case: str):
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
        entraid_table = f"{test_case}_entraid"
        self.write_existing_table(spark, entraid_data, entraid_table, "odw_harmonised_db", "odw-harmonised", entraid_table, "overwrite")

        expected_data = spark.createDataFrame(
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
        curated_table = f"{test_case}_entraid"
        with (
            mock.patch.object(EntraIDCurationProcess, "__init__", return_value=None),
            mock.patch.object(EntraIDCurationProcess, "HARMONISED_TABLE", entraid_table),
            mock.patch.object(EntraIDCurationProcess, "CURATED_TABLE", curated_table),
        ):
            inst = EntraIDCurationProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)
            actual_data = spark.table(f"odw_curated_db.{curated_table}")
            assert_etl_result_successful(expected_data, actual_data)

    def test__entraid_curation_process__run__with_no_existing_data(self):
        """
        - Given I have harmonised entraid data
        - When I call EntraIDCurationProcess.run
        - Then the curated entraid table should be created
        """
        self.assert_curation("t_ecp_r_wned")

    def test__entraid_curation_process__run__with_existing_data(self):
        """
        - Given I have harmonised entraid data and existing curated entraid data
        - When I call EntraIDCurationProcess.run
        - Then the curated entraid table should be created which overwrites the existing data
        """
        test_case = "t_ecp_r_wed"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_data = spark.createDataFrame(
            (
                {
                    "employeeId": "1",
                    "id": "x",
                    "givenName": "y",
                    "surname": "z",
                    "userPrincipalName": "a@example.gov.uk",
                }
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
        existing_table = f"{test_case}_entraid"
        self.write_existing_table(spark, existing_data, existing_table, "odw_curated_db", "odw-curated", existing_table, "overwrite")
        self.assert_curation(test_case)
