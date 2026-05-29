import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.service_user_harmonisation_process import ServiceUserHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _sb_service_user_schema():
    return StructType(
        [
            StructField("ServiceUserID", LongType(), True),
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("addressline1", StringType(), True),
            StructField("addressline2", StringType(), True),
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
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("Migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _horizon_case_involvement_schema():
    return StructType(
        [
            StructField("ContactId", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Address1", StringType(), True),
            StructField("Address2", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            StructField("PostCode", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("OrganisationName", StringType(), True),
            StructField("OrganisationTypeName", StringType(), True),
            StructField("TelephoneOffice", StringType(), True),
            StructField("TelephoneMobile", StringType(), True),
            StructField("Fax", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("case_number", StringType(), True),
            StructField("typeOfInvolvement", StringType(), True),
            StructField("expected_from", StringType(), True),
        ]
    )


def _horizon_nsip_data_schema():
    return StructType(
        [
            StructField("caseNodeId", StringType(), True),
            StructField("ApplicantFirstName", StringType(), True),
            StructField("ApplicantLastName", StringType(), True),
            StructField("AddressLine1", StringType(), True),
            StructField("AddressLine2", StringType(), True),
            StructField("AddressTown", StringType(), True),
            StructField("AddressCounty", StringType(), True),
            StructField("PostCode", StringType(), True),
            StructField("PromoterName", StringType(), True),
            StructField("ApplicantPhoneNumber", StringType(), True),
            StructField("ApplicantEmailAddress", StringType(), True),
            StructField("ApplicantWebAddress", StringType(), True),
            StructField("CaseReference", StringType(), True),
            StructField("expected_from", StringType(), True),
        ]
    )


def _horizon_nsip_relevant_representation_schema():
    return StructType(
        [
            StructField("ContactId", StringType(), True),
            StructField("CaseReference", StringType(), True),
            StructField("FullName", StringType(), True),
            StructField("BuildingNumber", StringType(), True),
            StructField("Street", StringType(), True),
            StructField("Town", StringType(), True),
            StructField("County", StringType(), True),
            StructField("PostCode", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("OrganisationName", StringType(), True),
            StructField("JobTitle", StringType(), True),
            StructField("PhoneNumber", StringType(), True),
            StructField("EmailAddress", StringType(), True),
            StructField("AgentContactId", StringType(), True),
            StructField("Agent_FullName", StringType(), True),
            StructField("Agent_BuildingNumber", StringType(), True),
            StructField("Agent_Street", StringType(), True),
            StructField("Agent_Town", StringType(), True),
            StructField("Agent_County", StringType(), True),
            StructField("Agent_Postcode", StringType(), True),
            StructField("Agent_Country", StringType(), True),
            StructField("Agent_OrganisationName", StringType(), True),
            StructField("Agent_JobTitle", StringType(), True),
            StructField("Agent_PhoneNumber", StringType(), True),
            StructField("Agent_EmailAddress", StringType(), True),
            StructField("preferredContactMethod", StringType(), True),
            StructField("expected_from", StringType(), True),
        ]
    )


def _service_user_output_schema():
    return StructType(
        [
            StructField("ServiceUserID", LongType(), True),
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("addressline1", StringType(), True),
            StructField("addressline2", StringType(), True),
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


class TestServiceUserHarmonisationProcess(ETLTestCase):
    def compare_harmonised_data(expected_data: DataFrame, actual_data: DataFrame):
        # Dropping ValidTo means the integration test does not prove the SCD2 latest/old row behaviour in the first integration test
        # It is ok though as the second int test checks ValidTo
        expected_data_cleaned = expected_data.drop("ServiceUserID", "IngestionDate", "ValidTo")
        actual_data_cleaned = actual_data.drop("ServiceUserID", "IngestionDate", "ValidTo")
        # If assert_dataframes_equal compares without ordering fine. If it is order sensitive this could fail:
        assert_dataframes_equal(expected_data_cleaned, actual_data_cleaned)

    def test__service_user_harmonisation_process__run__with_all_sources(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_suhp_r_was"

        service_user_table_name = f"{test_case}_service_user"
        sb_service_user_table_name = f"{test_case}_sb_service_user"
        horizon_case_involvement_table_name = f"{test_case}_horizon_case_involvement"
        horizon_nsip_data_table_name = f"{test_case}_horizon_nsip_data"
        horizon_representation_table_name = f"{test_case}_horizon_nsip_relevant_representation"

        sb_service_user = spark.createDataFrame(
            [
                (
                    99,
                    "SB-1",
                    "Mr",
                    "Service",
                    "Bus",
                    "1 Service Street",
                    "Flat 1",
                    "London",
                    "Greater London",
                    "SW1A 1AA",
                    "United Kingdom",
                    "Service Org",
                    "Company",
                    "Owner",
                    "01000",
                    "02000",
                    "03000",
                    "service@example.com",
                    "https://example.com",
                    "Appellant",
                    "CASE-001",
                    "Back Office",
                    "SB-1",
                    "0",
                    "Back Office",
                    "SRC-1",
                    "2024-01-01 00:00:00",
                    "",
                    "Y",
                )
            ],
            schema=_sb_service_user_schema(),
        )

        horizon_case_involvement = spark.createDataFrame(
            [
                (
                    "HZN-APP",
                    "Ms",
                    "Appeal",
                    "User",
                    "1 Horizon Street",
                    "Line 2",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Horizon Org",
                    "Organisation Type",
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    "APP-001",
                    "Appellant",
                    "2024-02-01 00:00:00",
                ),
                (
                    "HZN-AGENT",
                    "Mr",
                    "Agent",
                    "User",
                    "2 Horizon Street",
                    "Line 2",
                    "Bristol",
                    "Somerset",
                    "BS1 1AB",
                    "United Kingdom",
                    "Agent Org",
                    "Organisation Type",
                    "444",
                    "555",
                    "666",
                    "agent@example.com",
                    "APP-001",
                    "Agent",
                    "2024-02-01 00:00:00",
                ),
            ],
            schema=_horizon_case_involvement_schema(),
        )

        horizon_nsip_data = spark.createDataFrame(
            [
                (
                    "NSIP-1",
                    "Applicant",
                    "One",
                    "1 NSIP Street",
                    "Line 2",
                    "Cardiff",
                    "South Glamorgan",
                    "CF1 1AA",
                    "Promoter Ltd",
                    "777",
                    "applicant@example.com",
                    "https://applicant.example.com",
                    "NSIP-001",
                    "2024-03-01 00:00:00",
                )
            ],
            schema=_horizon_nsip_data_schema(),
        )

        horizon_representations = spark.createDataFrame(
            [
                (
                    "REP-1",
                    "REP-001",
                    "Represented Person",
                    "10",
                    "Rep Street",
                    "Manchester",
                    "Greater Manchester",
                    "M1 1AA",
                    "United Kingdom",
                    "Rep Org",
                    "Director",
                    "888",
                    "rep@example.com",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "Letter",
                    "2024-04-01 00:00:00",
                ),
                (
                    "REP-1",
                    "REP-001",
                    "Represented Person",
                    "10",
                    "Rep Street",
                    "Manchester",
                    "Greater Manchester",
                    "M1 1AA",
                    "United Kingdom",
                    "Rep Org",
                    "Director",
                    "888",
                    "rep@example.com",
                    "AGENT-1",
                    "Agent Person",
                    "20",
                    "Agent Street",
                    "Leeds",
                    "West Yorkshire",
                    "LS1 1AA",
                    "United Kingdom",
                    "Agent Org",
                    "Consultant",
                    "999",
                    "agent-rep@example.com",
                    "E-Mail",
                    "2024-04-01 00:00:00",
                ),
            ],
            schema=_horizon_nsip_relevant_representation_schema(),
        )

        self.write_existing_table(
            spark,
            sb_service_user,
            sb_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            sb_service_user_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            horizon_case_involvement,
            horizon_case_involvement_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_case_involvement_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            horizon_nsip_data,
            horizon_nsip_data_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_nsip_data_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            horizon_representations,
            horizon_representation_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_representation_table_name,
            "overwrite",
        )

        expected_service_user = spark.createDataFrame(
            [
                (
                    None,
                    "SB-1",
                    "Mr",
                    "Service",
                    "Bus",
                    "1 Service Street",
                    "Flat 1",
                    "London",
                    "Greater London",
                    "SW1A 1AA",
                    "United Kingdom",
                    "Service Org",
                    "Company",
                    "Owner",
                    "01000",
                    "02000",
                    "03000",
                    "service@example.com",
                    "https://example.com",
                    "Appellant",
                    "Appellant",
                    "CASE-001",
                    "Back Office",
                    "SB-1",
                    None,
                    "1",
                    "Back Office",
                    "2024-01-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "HZN-APP",
                    "Ms",
                    "Appeal",
                    "User",
                    "1 Horizon Street",
                    "Line 2",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Horizon Org",
                    "Organisation Type",
                    None,
                    "111",
                    "222",
                    "333",
                    "appeal@example.com",
                    None,
                    "Appellant",
                    "Appellant",
                    "APP-001",
                    "Horizon",
                    "HZN-APP",
                    None,
                    "0",
                    "Horizon",
                    "2024-02-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "HZN-AGENT",
                    "Mr",
                    "Agent",
                    "User",
                    "2 Horizon Street",
                    "Line 2",
                    "Bristol",
                    "Somerset",
                    "BS1 1AB",
                    "United Kingdom",
                    "Agent Org",
                    "Organisation Type",
                    None,
                    "444",
                    "555",
                    "666",
                    "agent@example.com",
                    None,
                    "Agent",
                    "Agent",
                    "APP-001",
                    "Horizon",
                    "HZN-AGENT",
                    None,
                    "0",
                    "Horizon",
                    "2024-02-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "NSIP-1",
                    None,
                    "Applicant",
                    "One",
                    "1 NSIP Street",
                    "Line 2",
                    "Cardiff",
                    "South Glamorgan",
                    "CF1 1AA",
                    None,
                    "Promoter Ltd",
                    None,
                    None,
                    "777",
                    None,
                    None,
                    "applicant@example.com",
                    "https://applicant.example.com",
                    "Applicant",
                    "Applicant",
                    "NSIP-001",
                    "Horizon",
                    "NSIP-1",
                    None,
                    "0",
                    "Horizon",
                    "2024-03-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "REP-1",
                    None,
                    None,
                    "Represented Person",
                    "10 Rep Street",
                    None,
                    "Manchester",
                    "Greater Manchester",
                    "M1 1AA",
                    "United Kingdom",
                    "Rep Org",
                    None,
                    "Director",
                    "888",
                    None,
                    None,
                    "rep@example.com",
                    None,
                    "RepresentationContact",
                    "RepresentationContact_Represented",
                    "REP-001",
                    "Horizon",
                    "REP-1",
                    "post",
                    "0",
                    "Horizon",
                    "2024-04-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "REP-1",
                    None,
                    None,
                    "Represented Person",
                    "10 Rep Street",
                    None,
                    "Manchester",
                    "Greater Manchester",
                    "M1 1AA",
                    "United Kingdom",
                    "Rep Org",
                    None,
                    "Director",
                    "888",
                    None,
                    None,
                    "rep@example.com",
                    None,
                    "RepresentationContact",
                    "RepresentationContact_Represented",
                    "REP-001",
                    "Horizon",
                    "REP-1",
                    None,
                    "0",
                    "Horizon",
                    "2024-04-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
                (
                    None,
                    "AGENT-1",
                    None,
                    None,
                    "Agent Person",
                    "20 Agent Street",
                    None,
                    "Leeds",
                    "West Yorkshire",
                    "LS1 1AA",
                    "United Kingdom",
                    "Agent Org",
                    None,
                    "Consultant",
                    "999",
                    None,
                    None,
                    "agent-rep@example.com",
                    None,
                    "RepresentationContact",
                    "RepresentationContact_Agent",
                    "REP-001",
                    "Horizon",
                    # Legacy notebook maps ContactID into sourceSuid for agent rows, not AgentContactId
                    "REP-1",
                    "email",
                    "0",
                    "Horizon",
                    "2024-04-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
            ],
            schema=_service_user_output_schema(),
        )

        with (
            mock.patch.object(ServiceUserHarmonisationProcess, "OUTPUT_TABLE", service_user_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "SERVICE_BUS_TABLE", sb_service_user_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_SERVICE_USER_TABLE", horizon_case_involvement_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_NSIP_PROJECT_TABLE", horizon_nsip_data_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_NSIP_REPRESENTATION_TABLE", horizon_representation_table_name),
        ):
            inst = ServiceUserHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="service_user", orchestration_orchestration_stage_name="harmonise")

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_harmonised_db.{service_user_table_name}")
            self.compare_harmonised_data(expected_service_user, actual_table_data)

    def test__service_user_harmonisation_process__run__same_composite_key_marks_latest_active_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_suhp_r_sck"

        service_user_table_name = f"{test_case}_service_user"
        sb_service_user_table_name = f"{test_case}_sb_service_user"
        horizon_case_involvement_table_name = f"{test_case}_horizon_case_involvement"
        horizon_nsip_data_table_name = f"{test_case}_horizon_nsip_data"
        horizon_representation_table_name = f"{test_case}_horizon_nsip_relevant_representation"

        sb_service_user = spark.createDataFrame(
            [
                (
                    99,
                    "SB-1",
                    "Mr",
                    "Older",
                    "Bus",
                    "1 Service Street",
                    "Flat 1",
                    "London",
                    "Greater London",
                    "SW1A 1AA",
                    "United Kingdom",
                    "Service Org",
                    "Company",
                    "Owner",
                    "01000",
                    "02000",
                    "03000",
                    "older@example.com",
                    "https://example.com",
                    "Appellant",
                    "CASE-001",
                    "Back Office",
                    "SB-1",
                    "0",
                    "Back Office",
                    "SRC-1",
                    "2024-01-01 00:00:00",
                    "",
                    "Y",
                ),
                (
                    99,
                    "SB-1",
                    "Mr",
                    "Newer",
                    "Bus",
                    "1 Service Street",
                    "Flat 1",
                    "London",
                    "Greater London",
                    "SW1A 1AA",
                    "United Kingdom",
                    "Service Org",
                    "Company",
                    "Owner",
                    "01000",
                    "02000",
                    "03000",
                    "newer@example.com",
                    "https://example.com",
                    "Appellant",
                    "CASE-001",
                    "Back Office",
                    "SB-1",
                    "0",
                    "Back Office",
                    "SRC-1",
                    "2024-02-01 00:00:00",
                    "",
                    "Y",
                ),
            ],
            schema=_sb_service_user_schema(),
        )

        empty_case_involvement = spark.createDataFrame([], schema=_horizon_case_involvement_schema())
        empty_nsip_data = spark.createDataFrame([], schema=_horizon_nsip_data_schema())
        empty_representations = spark.createDataFrame([], schema=_horizon_nsip_relevant_representation_schema())

        self.write_existing_table(
            spark,
            sb_service_user,
            sb_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            sb_service_user_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            empty_case_involvement,
            horizon_case_involvement_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_case_involvement_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            empty_nsip_data,
            horizon_nsip_data_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_nsip_data_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            empty_representations,
            horizon_representation_table_name,
            "odw_standardised_db",
            "odw-standardised",
            horizon_representation_table_name,
            "overwrite",
        )

        with (
            mock.patch.object(ServiceUserHarmonisationProcess, "OUTPUT_TABLE", service_user_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "SERVICE_BUS_TABLE", sb_service_user_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_SERVICE_USER_TABLE", horizon_case_involvement_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_NSIP_PROJECT_TABLE", horizon_nsip_data_table_name),
            mock.patch.object(ServiceUserHarmonisationProcess, "HZN_NSIP_REPRESENTATION_TABLE", horizon_representation_table_name),
        ):
            inst = ServiceUserHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="service_user", orchestration_orchestration_stage_name="harmonise")

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_harmonised_db.{service_user_table_name}")

            older = actual_table_data.where("firstname = 'Older'").collect()[0]
            newer = actual_table_data.where("firstname = 'Newer'").collect()[0]

            assert older["IsActive"] == "N"
            assert older["ValidTo"] is not None
            assert newer["IsActive"] == "Y"
            assert newer["ValidTo"] is None
