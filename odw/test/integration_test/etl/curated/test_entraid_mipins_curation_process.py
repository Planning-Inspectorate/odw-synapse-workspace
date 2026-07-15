import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.entraid_mipins_curation_process import (
    EntraIDMIPINSCurationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
import mock

pytestmark = pytest.mark.skip(reason="Curated MIPINS EntraID logic not implemented yet")


class TestEntraIDMIPINSCurationProcess(ETLTestCase):
    def assert_curation(self, test_case: str):
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
        entraid_table = f"{test_case}_entraid"
        self.write_existing_table(
            spark,
            entraid_data,
            entraid_table,
            "odw_harmonised_db",
            "odw-harmonised",
            entraid_table,
            "overwrite",
        )

        expected_output = spark.createDataFrame(
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
        output_table = f"{test_case}_entraid_curated_mipins"
        with (
            mock.patch.object(
                EntraIDMIPINSCurationProcess, "HARMONISED_TABLE", entraid_table
            ),
            mock.patch.object(
                EntraIDMIPINSCurationProcess, "CURATED_TABLE", output_table
            ),
        ):
            inst = EntraIDMIPINSCurationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="entraid_mipins",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)

        actual_output = spark.table(f"odw_curated_db.{output_table}")
        assert_dataframes_equal(expected_output, actual_output)

    def test__entraid_mipins_curation_process__run__with_no_existing_data(self):
        """
        - Given I have some entraid data in the harmonised layer
        - When I call EntraIDMIPINSCurationProcess.run
        - Then the curated mipins entraid table should be created
        """
        self.assert_curation("t_emcp_r_wned")

    def test__entraid_mipins_curation_process__run__with_existing_data(self):
        """
        - Given I have some entraid data in the harmonised layer with existing curated mipins entraid data
        - When I call EntraIDMIPINSCurationProcess.run
        - Then the curated mipins entraid table should be created, with the old curated data being overwritten
        """
        test_case = "t_emcp_r_wned"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_data = spark.createDataFrame(
            (
                {
                    "EmployeeEntraId": 1,
                    "ID": "someguidX",
                    "EmployeeID": 44442,
                    "GivenName": "Walter",
                    "Surname": "White",
                    "UserPrincipalName": "something@example.gov.uk",
                    "Migrated": "0",
                    "ODTSourceSystem": "EntraID",
                    "SourceSystemID": "1",
                    "IngestionDate": "2025-01-01 00:00:00",
                    "ValidTo": "2025-02-01 00:00:00",
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
        existing_table = f"{test_case}_entraid_curated_mipins"
        self.write_existing_table(
            spark,
            existing_data,
            existing_table,
            "odw_curated_db",
            "odw-curated",
            existing_table,
            "overwrite",
        )
        self.assert_curation(test_case)
