import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_service_user_curated_process import AppealServiceUserCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Curation logic not implemented yet")


def _harmonised_service_user_schema():
    return StructType(
        [
            StructField("ServiceUserID", LongType(), True),
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("addressCountry", StringType(), True),
            StructField("organisation", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("role", StringType(), True),
            StructField("telephoneNumber", StringType(), True),
            StructField("otherPhoneNumber", StringType(), True),
            StructField("faxNumber", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("webAddress", StringType(), True),
            StructField("serviceUserType", StringType(), True),
            StructField("serviceUserTypeInternal", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("contactMethod", StringType(), True),
            StructField("Migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _curated_appeal_service_user_schema():
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("addressCountry", StringType(), True),
            StructField("organisation", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("role", StringType(), True),
            StructField("telephoneNumber", StringType(), True),
            StructField("otherPhoneNumber", StringType(), True),
            StructField("faxNumber", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("webAddress", StringType(), True),
            StructField("serviceUserType", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("sourceSystem", StringType(), True),
        ]
    )


def _harmonised_service_user_row(**overrides):
    row = {
        "ServiceUserID": 1,
        "id": "SU-001",
        "salutation": "Mr",
        "firstName": "Appeal",
        "lastName": "User",
        "addressLine1": "1 Appeal Street",
        "addressLine2": "Flat 1",
        "addressTown": "Bristol",
        "addressCounty": "Somerset",
        "postcode": "BS1 1AA",
        "addressCountry": "United Kingdom",
        "organisation": "Appeal Org",
        "organisationType": "Company",
        "role": "Owner",
        "telephoneNumber": "111",
        "otherPhoneNumber": "222",
        "faxNumber": "333",
        "emailAddress": "appeal@example.com",
        "webAddress": "https://example.com",
        "serviceUserType": "Appellant",
        "serviceUserTypeInternal": "Appellant",
        "caseReference": "APP-001",
        "sourceSystem": "Horizon",
        "sourceSuid": "SU-001",
        "contactMethod": None,
        "Migrated": "0",
        "ODTSourceSystem": "Horizon",
        "IngestionDate": "2024-01-01 00:00:00",
        "ValidTo": None,
        "RowID": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


class TestAppealServiceUserCuratedProcess(ETLTestCase):
    def compare_curated_data(expected_data: DataFrame, actual_data: DataFrame):
        assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_service_user_curated_process__run__with_eligible_and_ineligible_service_users(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_asucp_r_wei"

        source_service_user_table_name = f"{test_case}_service_user"
        output_appeal_service_user_table_name = f"{test_case}_appeal_service_user"

        harmonised_service_user = spark.createDataFrame(
            [
                _harmonised_service_user_row(
                    ServiceUserID=1,
                    id="APP-HZN",
                    firstName="Horizon",
                    lastName="Appellant",
                    serviceUserType="Appellant",
                    caseReference="APP-001",
                    sourceSystem="Horizon",
                    sourceSuid="APP-HZN",
                    IsActive="Y",
                ),
                _harmonised_service_user_row(
                    ServiceUserID=2,
                    id="APP-BO",
                    firstName="Back Office",
                    lastName="Appellant",
                    serviceUserType="Appellant",
                    caseReference="APP-002",
                    sourceSystem="back-office-appeals",
                    sourceSuid="APP-BO",
                    IsActive="Y",
                ),
                _harmonised_service_user_row(
                    ServiceUserID=3,
                    id="BO-AGENT",
                    firstName="Back Office",
                    lastName="Agent",
                    serviceUserType="Agent",
                    caseReference="APP-003",
                    sourceSystem="back-office-appeals",
                    sourceSuid="BO-AGENT",
                    IsActive="Y",
                ),
                _harmonised_service_user_row(
                    ServiceUserID=4,
                    id="HZN-AGENT",
                    firstName="Horizon",
                    lastName="Agent",
                    serviceUserType="Agent",
                    caseReference="APP-004",
                    sourceSystem="Horizon",
                    sourceSuid="HZN-AGENT",
                    IsActive="Y",
                ),
                _harmonised_service_user_row(
                    ServiceUserID=5,
                    id="HZN-REP",
                    firstName="Horizon",
                    lastName="Rep",
                    serviceUserType="RepresentationContact",
                    caseReference="APP-005",
                    sourceSystem="Horizon",
                    sourceSuid="HZN-REP",
                    IsActive="Y",
                ),
            ],
            schema=_harmonised_service_user_schema(),
        )

        self.write_existing_table(
            spark,
            harmonised_service_user,
            source_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            source_service_user_table_name,
            "overwrite",
        )

        expected_appeal_service_user = spark.createDataFrame(
            [
                (
                    "APP-HZN",
                    "Mr",
                    "Horizon",
                    "Appellant",
                    "1 Appeal Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Appeal Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "https://example.com",
                    "Appellant",
                    "APP-001",
                    "APP-HZN",
                    "Horizon",
                ),
                (
                    "APP-BO",
                    "Mr",
                    "Back Office",
                    "Appellant",
                    "1 Appeal Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Appeal Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "https://example.com",
                    "Appellant",
                    "APP-002",
                    "APP-BO",
                    "back-office-appeals",
                ),
                (
                    "BO-AGENT",
                    "Mr",
                    "Back Office",
                    "Agent",
                    "1 Appeal Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Appeal Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "https://example.com",
                    "Agent",
                    "APP-003",
                    "BO-AGENT",
                    "back-office-appeals",
                ),
            ],
            schema=_curated_appeal_service_user_schema(),
        )

        with (
            mock.patch.object(AppealServiceUserCuratedProcess, "SOURCE_TABLE", source_service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedProcess, "OUTPUT_TABLE", output_appeal_service_user_table_name),
        ):
            inst = AppealServiceUserCuratedProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="appeal_service_user", orchestration_stage_name="curate")

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_curated_db.{output_appeal_service_user_table_name}")
            self.compare_curated_data(expected_appeal_service_user, actual_table_data)

    def test__appeal_service_user_curated_process__run__preserves_duplicate_eligible_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_asucp_r_pder"

        source_service_user_table_name = f"{test_case}_service_user"
        output_appeal_service_user_table_name = f"{test_case}_appeal_service_user"

        duplicate_row = _harmonised_service_user_row(
            ServiceUserID=1,
            id="DUP-APP",
            firstName="Duplicate",
            lastName="Appellant",
            serviceUserType="Appellant",
            caseReference="APP-DUP",
            sourceSystem="Horizon",
            sourceSuid="DUP-APP",
        )

        harmonised_service_user = spark.createDataFrame(
            [
                duplicate_row,
                duplicate_row,
            ],
            schema=_harmonised_service_user_schema(),
        )

        self.write_existing_table(
            spark,
            harmonised_service_user,
            source_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            source_service_user_table_name,
            "overwrite",
        )

        expected_appeal_service_user = spark.createDataFrame(
            [
                (
                    "DUP-APP",
                    "Mr",
                    "Duplicate",
                    "Appellant",
                    "1 Appeal Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Appeal Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "https://example.com",
                    "Appellant",
                    "APP-DUP",
                    "DUP-APP",
                    "Horizon",
                ),
                (
                    "DUP-APP",
                    "Mr",
                    "Duplicate",
                    "Appellant",
                    "1 Appeal Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Appeal Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "https://example.com",
                    "Appellant",
                    "APP-DUP",
                    "DUP-APP",
                    "Horizon",
                ),
            ],
            schema=_curated_appeal_service_user_schema(),
        )

        with (
            mock.patch.object(AppealServiceUserCuratedProcess, "SOURCE_TABLE", source_service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedProcess, "OUTPUT_TABLE", output_appeal_service_user_table_name),
        ):
            inst = AppealServiceUserCuratedProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="appeal_service_user", orchestration_stage_name="curate")

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_curated_db.{output_appeal_service_user_table_name}")
            self.compare_curated_data(expected_appeal_service_user, actual_table_data)
