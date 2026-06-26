import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.service_user_harmonisation_process import ServiceUserHarmonisationProcess
from odw.core.etl.metadata_manager import MetadataManager
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.skip(reason="Harmonisation logic not implemented yet")


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


def _sb_service_user_row(**overrides):
    row = {
        "ServiceUserID": 99,
        "id": "SB-1",
        "salutation": "Mr",
        "firstname": "Service",
        "lastname": "Bus",
        "addressline1": "1 Service Street",
        "addressline2": "Flat 1",
        "addressTown": "London",
        "addressCounty": "Greater London",
        "postcode": "SW1A 1AA",
        "addressCountry": "United Kingdom",
        "organisation": "Service Org",
        "organisationType": "Company",
        "role": "Owner",
        "telephoneNumber": "01000",
        "otherPhoneNumber": "02000",
        "faxNumber": "03000",
        "emailAddress": "service@example.com",
        "webAddress": "https://example.com",
        "serviceUserType": "Appellant",
        "caseReference": "CASE-001",
        "sourceSystem": "Back Office",
        "sourceSuid": "SB-1",
        "Migrated": "0",
        "ODTSourceSystem": "Back Office",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2024-01-01 00:00:00",
        "ValidTo": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


def _horizon_case_involvement_row(**overrides):
    row = {
        "ContactId": "HZN-1",
        "Title": "Ms",
        "FirstName": "Appeal",
        "LastName": "User",
        "Address1": "1 Horizon Street",
        "Address2": "Line 2",
        "City": "Bristol",
        "County": "Somerset",
        "PostCode": "BS1 1AA",
        "Country": "United Kingdom",
        "OrganisationName": "Horizon Org",
        "OrganisationTypeName": "Agent",
        "TelephoneOffice": "111",
        "TelephoneMobile": "222",
        "Fax": "333",
        "Email": "appeal@example.com",
        "case_number": "APP-001",
        "typeOfInvolvement": "Appellant",
        "expected_from": "2024-02-01 00:00:00",
    }
    row.update(overrides)
    return row


def _horizon_nsip_data_row(**overrides):
    row = {
        "caseNodeId": "NSIP-1",
        "ApplicantFirstName": "Applicant",
        "ApplicantLastName": "One",
        "AddressLine1": "1 NSIP Street",
        "AddressLine2": "Line 2",
        "AddressTown": "Cardiff",
        "AddressCounty": "South Glamorgan",
        "PostCode": "CF1 1AA",
        "PromoterName": "Promoter Ltd",
        "ApplicantPhoneNumber": "444",
        "ApplicantEmailAddress": "applicant@example.com",
        "ApplicantWebAddress": "https://applicant.example.com",
        "CaseReference": "NSIP-001",
        "expected_from": "2024-03-01 00:00:00",
    }
    row.update(overrides)
    return row


def _horizon_relevant_representation_row(**overrides):
    row = {
        "ContactId": "REP-1",
        "CaseReference": "REP-001",
        "FullName": "Represented Person",
        "BuildingNumber": "10",
        "Street": "Rep Street",
        "Town": "Manchester",
        "County": "Greater Manchester",
        "PostCode": "M1 1AA",
        "Country": "United Kingdom",
        "OrganisationName": "Rep Org",
        "JobTitle": "Director",
        "PhoneNumber": "555",
        "EmailAddress": "rep@example.com",
        "AgentContactId": None,
        "Agent_FullName": None,
        "Agent_BuildingNumber": None,
        "Agent_Street": None,
        "Agent_Town": None,
        "Agent_County": None,
        "Agent_Postcode": None,
        "Agent_Country": None,
        "Agent_OrganisationName": None,
        "Agent_JobTitle": None,
        "Agent_PhoneNumber": None,
        "Agent_EmailAddress": None,
        "preferredContactMethod": "Letter",
        "expected_from": "2024-04-01 00:00:00",
    }
    row.update(overrides)
    return row


def _source_data(spark, service_bus_rows=None, case_involvement_rows=None, nsip_rows=None, representation_rows=None):
    return {
        "service_bus_data": spark.createDataFrame(service_bus_rows or [], schema=_sb_service_user_schema()),
        "horizon_case_involvement_data": spark.createDataFrame(case_involvement_rows or [], schema=_horizon_case_involvement_schema()),
        "horizon_nsip_data": spark.createDataFrame(nsip_rows or [], schema=_horizon_nsip_data_schema()),
        "horizon_nsip_relevant_representation_data": spark.createDataFrame(
            representation_rows or [],
            schema=_horizon_nsip_relevant_representation_schema(),
        ),
        "target_exists": False,
    }


class TestServiceUserHarmonisationProcess(SparkTestCase):
    def test__service_user_harmonisation_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = ServiceUserHarmonisationProcess(spark)

        assert inst.get_name() == "Service User Harmonisation Process"

    def test__service_user_harmonisation_process__process__outputs_expected_legacy_columns_only(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_bus_rows=[_sb_service_user_row()])

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "ServiceUserID",
            "id",
            "salutation",
            "firstname",
            "lastname",
            "addressline1",
            "addressline2",
            "addressTown",
            "addressCounty",
            "postcode",
            "addressCountry",
            "organisation",
            "organisationType",
            "role",
            "telephoneNumber",
            "otherPhoneNumber",
            "faxNumber",
            "emailAddress",
            "webAddress",
            "serviceUserType",
            "serviceUserTypeInternal",
            "caseReference",
            "sourceSystem",
            "sourceSuid",
            "contactMethod",
            "Migrated",
            "ODTSourceSystem",
            "IngestionDate",
            "ValidTo",
            "RowID",
            "IsActive",
        ]

    def test__service_user_harmonisation_process__process__maps_service_bus_source_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_bus_rows=[_sb_service_user_row()])

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["id"] == "SB-1"
        assert row["firstname"] == "Service"
        assert row["lastname"] == "Bus"
        assert row["serviceUserType"] == "Appellant"
        assert row["serviceUserTypeInternal"] == "Appellant"
        assert row["caseReference"] == "CASE-001"
        assert row["sourceSystem"] == "Back Office"
        assert row["sourceSuid"] == "SB-1"
        assert row["contactMethod"] is None
        assert row["Migrated"] == "1"
        assert row["ValidTo"] is None
        assert row["RowID"] == ""
        assert row["IsActive"] == "Y"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__service_user_harmonisation_process__process__maps_horizon_appellant_and_agent_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            case_involvement_rows=[
                _horizon_case_involvement_row(ContactId="HZN-APP", typeOfInvolvement="Appellant"),
                _horizon_case_involvement_row(ContactId="HZN-AGENT", typeOfInvolvement="Agent"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        appellant = df.where(F.col("id") == "HZN-APP").collect()[0]
        agent = df.where(F.col("id") == "HZN-AGENT").collect()[0]

        assert appellant["serviceUserType"] == "Appellant"
        assert appellant["serviceUserTypeInternal"] == "Appellant"
        assert appellant["sourceSystem"] == "Horizon"
        assert appellant["sourceSuid"] == "HZN-APP"
        assert appellant["Migrated"] == "0"

        assert agent["serviceUserType"] == "Agent"
        assert agent["serviceUserTypeInternal"] == "Agent"
        assert agent["sourceSystem"] == "Horizon"
        assert agent["sourceSuid"] == "HZN-AGENT"

        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__service_user_harmonisation_process__process__maps_legacy_appellant_typo_values(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            case_involvement_rows=[
                _horizon_case_involvement_row(ContactId="APP-1", typeOfInvolvement="Appellant"),
                _horizon_case_involvement_row(ContactId="APP-2", typeOfInvolvement="tAppellant"),
                _horizon_case_involvement_row(ContactId="APP-3", typeOfInvolvement="Apellant"),
                _horizon_case_involvement_row(ContactId="IGNORE-1", typeOfInvolvement="InterestedParty"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "APP-1").count() == 1
        assert df.where(F.col("id") == "APP-2").count() == 1
        assert df.where(F.col("id") == "APP-3").count() == 1
        assert df.where(F.col("id") == "IGNORE-1").count() == 0
        assert df.where(F.col("serviceUserType") == "Appellant").count() == 3
        assert result.metadata.insert_count == 3

    def test__service_user_harmonisation_process__process__filters_horizon_case_involvement_to_latest_expected_from_and_non_null_contact(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            case_involvement_rows=[
                _horizon_case_involvement_row(ContactId="OLD", expected_from="2024-01-01 00:00:00"),
                _horizon_case_involvement_row(ContactId="LATEST", expected_from="2024-02-01 00:00:00"),
                _horizon_case_involvement_row(ContactId=None, expected_from="2024-02-01 00:00:00"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "OLD").count() == 0
        assert df.where(F.col("id") == "LATEST").count() == 1
        assert df.where(F.col("id").isNull()).count() == 0

    def test__service_user_harmonisation_process__process__maps_nsip_project_applicant_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, nsip_rows=[_horizon_nsip_data_row()])

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["id"] == "NSIP-1"
        assert row["firstname"] == "Applicant"
        assert row["lastname"] == "One"
        assert row["organisation"] == "Promoter Ltd"
        assert row["serviceUserType"] == "Applicant"
        assert row["serviceUserTypeInternal"] == "Applicant"
        assert row["caseReference"] == "NSIP-001"
        assert row["sourceSystem"] == "Horizon"
        assert row["sourceSuid"] == "NSIP-1"
        assert row["Migrated"] == "0"
        assert row["IsActive"] == "Y"

    def test__service_user_harmonisation_process__process__filters_nsip_project_to_latest_expected_from_and_non_null_case_node_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            nsip_rows=[
                _horizon_nsip_data_row(caseNodeId="OLD-NSIP", expected_from="2024-01-01 00:00:00"),
                _horizon_nsip_data_row(caseNodeId="LATEST-NSIP", expected_from="2024-03-01 00:00:00"),
                _horizon_nsip_data_row(caseNodeId=None, expected_from="2024-03-01 00:00:00"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "OLD-NSIP").count() == 0
        assert df.where(F.col("id") == "LATEST-NSIP").count() == 1
        assert df.where(F.col("id").isNull()).count() == 0

    def test__service_user_harmonisation_process__process__maps_relevant_representation_represented_contact_method_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            representation_rows=[
                _horizon_relevant_representation_row(ContactId="REP-LETTER", preferredContactMethod="Letter"),
                _horizon_relevant_representation_row(ContactId="REP-EMAIL", preferredContactMethod="E-Mail"),
                _horizon_relevant_representation_row(ContactId="REP-PORTAL", preferredContactMethod="Portal"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("id") == "REP-LETTER") & (F.col("contactMethod") == "post")).count() == 1
        assert df.where((F.col("id") == "REP-EMAIL") & (F.col("contactMethod") == "email")).count() == 1
        assert df.where((F.col("id") == "REP-PORTAL") & (F.col("contactMethod") == "portal")).count() == 1
        assert df.where(F.col("serviceUserTypeInternal") == "RepresentationContact_Represented").count() == 3

    def test__service_user_harmonisation_process__process__maps_relevant_representation_agent_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            representation_rows=[
                _horizon_relevant_representation_row(
                    ContactId="REP-1",
                    AgentContactId="AGENT-1",
                    Agent_FullName="Agent Person",
                    Agent_BuildingNumber="20",
                    Agent_Street="Agent Street",
                    Agent_Town="Leeds",
                    Agent_County="West Yorkshire",
                    Agent_Postcode="LS1 1AA",
                    Agent_Country="United Kingdom",
                    Agent_OrganisationName="Agent Org",
                    Agent_JobTitle="Consultant",
                    Agent_PhoneNumber="999",
                    Agent_EmailAddress="agent@example.com",
                    preferredContactMethod="E-Mail",
                )
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where(F.col("id") == "AGENT-1").collect()[0]

        assert row["lastname"] == "Agent Person"
        assert row["addressline1"] == "20 Agent Street"
        assert row["addressTown"] == "Leeds"
        assert row["organisation"] == "Agent Org"
        assert row["role"] == "Consultant"
        assert row["telephoneNumber"] == "999"
        assert row["emailAddress"] == "agent@example.com"
        assert row["serviceUserType"] == "RepresentationContact"
        assert row["serviceUserTypeInternal"] == "RepresentationContact_Agent"
        assert row["contactMethod"] == "email"

    def test__service_user_harmonisation_process__process__filters_representations_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            representation_rows=[
                _horizon_relevant_representation_row(ContactId="REP-VALID", AgentContactId=None),
                _horizon_relevant_representation_row(ContactId=None, AgentContactId=None),
                _horizon_relevant_representation_row(ContactId="REP-NO-CASE", CaseReference=None, AgentContactId=None),
                _horizon_relevant_representation_row(ContactId="REP-WITH-AGENT", AgentContactId="AGENT-VALID"),
                _horizon_relevant_representation_row(ContactId="REP-AGENT-NO-CASE", CaseReference=None, AgentContactId="AGENT-NO-CASE"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("id") == "REP-VALID") & (F.col("serviceUserTypeInternal") == "RepresentationContact_Represented")).count() == 1
        assert df.where((F.col("id") == "REP-WITH-AGENT") & (F.col("serviceUserTypeInternal") == "RepresentationContact_Represented")).count() == 1
        assert df.where((F.col("id") == "AGENT-VALID") & (F.col("serviceUserTypeInternal") == "RepresentationContact_Agent")).count() == 1
        assert df.where(F.col("id").isNull()).count() == 0
        assert df.where(F.col("id") == "REP-NO-CASE").count() == 0
        assert df.where(F.col("id") == "AGENT-NO-CASE").count() == 0

    def test__service_user_harmonisation_process__process__filters_representations_to_latest_expected_from(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            representation_rows=[
                _horizon_relevant_representation_row(ContactId="OLD-REP", expected_from="2024-01-01 00:00:00"),
                _horizon_relevant_representation_row(ContactId="LATEST-REP", expected_from="2024-04-01 00:00:00"),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "OLD-REP").count() == 0
        assert df.where(F.col("id") == "LATEST-REP").count() == 1

    def test__service_user_harmonisation_process__process__calculates_service_user_id_active_and_valid_to_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_bus_rows=[
                _sb_service_user_row(
                    id="SB-1",
                    caseReference="CASE-001",
                    serviceUserType="Appellant",
                    firstname="Older",
                    IngestionDate="2024-01-01 00:00:00",
                ),
                _sb_service_user_row(
                    id="SB-1",
                    caseReference="CASE-001",
                    serviceUserType="Appellant",
                    firstname="Newer",
                    IngestionDate="2024-02-01 00:00:00",
                ),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        older = df.where(F.col("firstname") == "Older").collect()[0]
        newer = df.where(F.col("firstname") == "Newer").collect()[0]

        assert older["IsActive"] == "N"
        assert older["ValidTo"] is not None
        assert newer["IsActive"] == "Y"
        assert newer["ValidTo"] is None
        assert older["ServiceUserID"] != newer["ServiceUserID"]

    def test__service_user_harmonisation_process__process__preserves_non_blank_service_bus_valid_to_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_bus_rows=[
                _sb_service_user_row(
                    ValidTo="2024-05-01 00:00:00",
                ),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["ValidTo"] is not None
        assert str(row["ValidTo"]).startswith("2024-05-01 00:00:00")

    def test__service_user_harmonisation_process__process__drops_exact_duplicate_rows_like_legacy(self):
        # Legacy notebook applies drop_duplicates() after calculating final fields
        spark = PytestSparkSessionUtil().get_spark_session()
        duplicate_row = _sb_service_user_row()

        source_data = _source_data(
            spark,
            service_bus_rows=[
                duplicate_row,
                duplicate_row,
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__service_user_harmonisation_process__process__same_id_and_case_reference_but_different_type_are_separate_active_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_bus_rows=[
                _sb_service_user_row(
                    id="SB-1",
                    caseReference="CASE-001",
                    serviceUserType="Appellant",
                    firstname="Appellant User",
                    IngestionDate="2024-01-01 00:00:00",
                ),
                _sb_service_user_row(
                    id="SB-1",
                    caseReference="CASE-001",
                    serviceUserType="Agent",
                    firstname="Agent User",
                    IngestionDate="2024-01-01 00:00:00",
                ),
            ],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where((F.col("serviceUserType") == "Appellant") & (F.col("IsActive") == "Y")).count() == 1
        assert df.where((F.col("serviceUserType") == "Agent") & (F.col("IsActive") == "Y")).count() == 1

    def test__service_user_harmonisation_process__process__initial_load_uses_overwrite_and_inserts_all_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_bus_rows=[_sb_service_user_row()],
            case_involvement_rows=[_horizon_case_involvement_row(ContactId="HZN-1")],
            nsip_rows=[_horizon_nsip_data_row(caseNodeId="NSIP-1")],
            representation_rows=[_horizon_relevant_representation_row(ContactId="REP-1")],
        )

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 4
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "delta"
        assert data_to_write[inst.OUTPUT_TABLE]["partition_by"] == ["IsActive"]
        assert result.metadata.insert_count == 4
        assert result.metadata.update_count == 0

    def test__service_user_harmonisation_process__process__empty_sources_return_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark)

        inst = ServiceUserHarmonisationProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__service_user_harmonisation_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(
            spark,
            service_bus_rows=[_sb_service_user_row()],
            case_involvement_rows=[_horizon_case_involvement_row(ContactId="HZN-1")],
            nsip_rows=[_horizon_nsip_data_row(caseNodeId="NSIP-1")],
            representation_rows=[_horizon_relevant_representation_row(ContactId="REP-1")],
        )

        inst = ServiceUserHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
            mock.patch.object(MetadataManager, "__init__", return_value=None),
            mock.patch.object(MetadataManager, "create", return_value=None),
            mock.patch.object(MetadataManager, "update", return_value=None),
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 4
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 4
        assert result.metadata.update_count == 0
        assert df.where(F.col("IsActive") == "Y").count() == 4
