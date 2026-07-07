import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.nsip_representation_harmonisation_process import (
    NsipRepresentationHarmonisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
from datetime import datetime
import pyspark.sql.types as T
import mock


class TestNSIPRepresentationHarmonisation(ETLTestCase):
    def test__nsip_representation_harmonisation_process__run__combines_sources_and_derives_reference_and_migrated(
        self,
    ):
        test_case = "t_nrhp_r_csadram"
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = spark.createDataFrame(
            [
                (
                    1,
                    10,
                    "REF-1",
                    "EX-1",
                    "EN010001",
                    100,
                    "Valid",
                    "orig1",
                    False,
                    None,
                    None,
                    None,
                    "Person",
                    "repd1",
                    "repr1",
                    "reg1",
                    "Type1",
                    "2025-01-01",
                    ["A1"],
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
                    "1",
                    "ODT",
                    "SRC1",
                    "2025-01-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("NSIPRepresentationID", T.IntegerType(), True),
                    T.StructField("representationId", T.IntegerType(), True),
                    T.StructField("referenceId", T.StringType(), True),
                    T.StructField("examinationLibraryRef", T.StringType(), True),
                    T.StructField("caseRef", T.StringType(), True),
                    T.StructField("caseId", T.IntegerType(), True),
                    T.StructField("status", T.StringType(), True),
                    T.StructField("originalRepresentation", T.StringType(), True),
                    T.StructField("redacted", T.BooleanType(), True),
                    T.StructField("redactedRepresentation", T.StringType(), True),
                    T.StructField("redactedBy", T.StringType(), True),
                    T.StructField("redactedNotes", T.StringType(), True),
                    T.StructField("representationFrom", T.StringType(), True),
                    T.StructField("representedId", T.StringType(), True),
                    T.StructField("representativeId", T.StringType(), True),
                    T.StructField("registerFor", T.StringType(), True),
                    T.StructField("representationType", T.StringType(), True),
                    T.StructField("dateReceived", T.StringType(), True),
                    T.StructField("attachmentIds", T.ArrayType(T.StringType()), True),
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("organisationname", T.StringType(), True),
                    T.StructField("jobtitle", T.StringType(), True),
                    T.StructField("fullname", T.StringType(), True),
                    T.StructField("phonenumber", T.StringType(), True),
                    T.StructField("emailaddress", T.StringType(), True),
                    T.StructField("buildingnumber", T.StringType(), True),
                    T.StructField("street", T.StringType(), True),
                    T.StructField("town", T.StringType(), True),
                    T.StructField("county", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                    T.StructField("postcode", T.StringType(), True),
                    T.StructField("attendprelimmeeting", T.StringType(), True),
                    T.StructField("AgentFullName", T.StringType(), True),
                    T.StructField("AgentOrganisationName", T.StringType(), True),
                    T.StructField("agent_phonenumber", T.StringType(), True),
                    T.StructField("agent_emailaddress", T.StringType(), True),
                    T.StructField("AgentBuildingNumber", T.StringType(), True),
                    T.StructField("AgentStreet", T.StringType(), True),
                    T.StructField("AgentTown", T.StringType(), True),
                    T.StructField("AgentCounty", T.StringType(), True),
                    T.StructField("AgentCountry", T.StringType(), True),
                    T.StructField("AgentPostcode", T.StringType(), True),
                    T.StructField("representatcompacqhearing", T.StringType(), True),
                    T.StructField("representatissuehearing", T.StringType(), True),
                    T.StructField("representatopenfloorhearing", T.StringType(), True),
                    T.StructField("submitlaterreps", T.StringType(), True),
                    T.StructField("webreference", T.StringType(), True),
                    T.StructField("owneroroccupier", T.StringType(), True),
                    T.StructField("powertosell", T.StringType(), True),
                    T.StructField("entitledtoclaim", T.StringType(), True),
                    T.StructField("other", T.StringType(), True),
                    T.StructField("descriptionifother", T.StringType(), True),
                    T.StructField("preferredcontactmethod", T.StringType(), True),
                    T.StructField("Migrated", T.StringType(), True),
                    T.StructField("ODTSourceSystem", T.StringType(), True),
                    T.StructField("SourceSystemID", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("RowID", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )
        service_bus_table = f"{test_case}_sb_nsip_representation"
        self.write_existing_table(
            spark,
            service_bus_data,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

        horizon_data = spark.createDataFrame(
            [
                (
                    20,
                    "EN010002",
                    200,
                    "New",
                    "orig2",
                    "red2",
                    "user2",
                    "notes2",
                    "Organisation",
                    "contact2",
                    "agent2",
                    "Type2",
                    "2025-02-01",
                    "A2",
                    "caseu2",
                    "org2",
                    "job2",
                    "full2",
                    "phone2",
                    "mail2@test.com",
                    "12",
                    "street2",
                    "town2",
                    "county2",
                    "country2",
                    "pc2",
                    "yes",
                    "agent full",
                    "agent org",
                    "aphone",
                    "amail@test.com",
                    "abuild",
                    "astreet",
                    "atown",
                    "acounty",
                    "acountry",
                    "apost",
                    "yes",
                    "no",
                    "yes",
                    "later",
                    "web2",
                    "owner2",
                    "power2",
                    "claim2",
                    "other2",
                    "desc-other2",
                    "email",
                    datetime(2025, 1, 1),
                ),
                (
                    20,
                    "EN010002",
                    200,
                    "New",
                    "orig2",
                    "red2",
                    "user2",
                    "notes2",
                    "Organisation",
                    "contact2",
                    "agent2",
                    "Type2",
                    "2025-02-01",
                    "A3",
                    "caseu2",
                    "org2",
                    "job2",
                    "full2",
                    "phone2",
                    "mail2@test.com",
                    "12",
                    "street2",
                    "town2",
                    "county2",
                    "country2",
                    "pc2",
                    "yes",
                    "agent full",
                    "agent org",
                    "aphone",
                    "amail@test.com",
                    "abuild",
                    "astreet",
                    "atown",
                    "acounty",
                    "acountry",
                    "apost",
                    "yes",
                    "no",
                    "yes",
                    "later",
                    "web2",
                    "owner2",
                    "power2",
                    "claim2",
                    "other2",
                    "desc-other2",
                    "email",
                    datetime(2025, 1, 1),
                ),
            ],
            T.StructType(
                [
                    T.StructField("relevantrepid", T.IntegerType(), True),
                    T.StructField("casereference", T.StringType(), True),
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("RelevantRepStatus", T.StringType(), True),
                    T.StructField("representationoriginal", T.StringType(), True),
                    T.StructField("representationredacted", T.StringType(), True),
                    T.StructField("redactedBy", T.StringType(), True),
                    T.StructField("notes", T.StringType(), True),
                    T.StructField("RelRepOnBehalfOf", T.StringType(), True),
                    T.StructField("contactId", T.StringType(), True),
                    T.StructField("agentcontactid", T.StringType(), True),
                    T.StructField("RelRepOrganisation", T.StringType(), True),
                    T.StructField("dateReceived", T.StringType(), True),
                    T.StructField("attachmentId", T.StringType(), True),
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("organisationname", T.StringType(), True),
                    T.StructField("jobtitle", T.StringType(), True),
                    T.StructField("fullname", T.StringType(), True),
                    T.StructField("phonenumber", T.StringType(), True),
                    T.StructField("emailaddress", T.StringType(), True),
                    T.StructField("buildingnumber", T.StringType(), True),
                    T.StructField("street", T.StringType(), True),
                    T.StructField("town", T.StringType(), True),
                    T.StructField("county", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                    T.StructField("postcode", T.StringType(), True),
                    T.StructField("attendprelimmeeting", T.StringType(), True),
                    T.StructField("agent_fullname", T.StringType(), True),
                    T.StructField("agent_organisationname", T.StringType(), True),
                    T.StructField("agent_phonenumber", T.StringType(), True),
                    T.StructField("agent_emailaddress", T.StringType(), True),
                    T.StructField("agent_buildingnumber", T.StringType(), True),
                    T.StructField("agent_street", T.StringType(), True),
                    T.StructField("agent_town", T.StringType(), True),
                    T.StructField("Agent_County", T.StringType(), True),
                    T.StructField("Agent_Country", T.StringType(), True),
                    T.StructField("agent_postcode", T.StringType(), True),
                    T.StructField("representatcompacqhearing", T.StringType(), True),
                    T.StructField("representatissuehearing", T.StringType(), True),
                    T.StructField("representatopenfloorhearing", T.StringType(), True),
                    T.StructField("submitlaterreps", T.StringType(), True),
                    T.StructField("webreference", T.StringType(), True),
                    T.StructField("owneroroccupier", T.StringType(), True),
                    T.StructField("powertosell", T.StringType(), True),
                    T.StructField("entitledtoclaim", T.StringType(), True),
                    T.StructField("other", T.StringType(), True),
                    T.StructField("descriptionifother", T.StringType(), True),
                    T.StructField("preferredcontactmethod", T.StringType(), True),
                    T.StructField("ingested_datetime", T.TimestampType(), True),
                ]
            ),
        )
        horizon_table = f"{test_case}_horizon_nsip_relevant_representation"
        self.write_existing_table(
            spark,
            horizon_data,
            horizon_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_table,
            "overwrite",
        )

        source_system_data = spark.createDataFrame(
            [("SRC_CASEWORK", "Casework", "Y", None, "2025-02-01 00:00:00")],
            schema=T.StructType(
                [
                    T.StructField("SourceSystemID", T.StringType(), True),
                    T.StructField("Description", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                ]
            ),
        )
        source_system_table = f"{test_case}_main_sourcesystem_fact"
        self.write_existing_table(
            spark,
            source_system_data,
            source_system_table,
            "odw_harmonised_db",
            "odw-harmonised",
            source_system_table,
            "overwrite",
        )

        output_table = f"{test_case}_nsip_representation"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_representation_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipRepresentationHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                NsipRepresentationHarmonisationProcess,
                "HORIZON_TABLE",
                f"odw_standardised_db.{horizon_table}",
            ),
            mock.patch.object(
                NsipRepresentationHarmonisationProcess,
                "SOURCE_SYSTEM_TABLE",
                f"odw_harmonised_db.{source_system_table}",
            ),
            mock.patch.object(
                NsipRepresentationHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = NsipRepresentationHarmonisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_representation",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        rows = [row.asDict(recursive=True) for row in actual_df.collect()]
        sb_rows = [row for row in rows if row["representationId"] == 10]
        horizon_rows = [row for row in rows if row["representationId"] == 20]

        assert actual_df.count() == 3
        assert len(sb_rows) == 1
        assert sb_rows[0]["Migrated"] == "1"
        assert len(horizon_rows) == 2
        for row in horizon_rows:
            assert row["Migrated"] == "0"
            assert row["referenceId"] == "EN010002-20"
            assert row["redacted"] is True
            assert sorted(row["attachmentIds"]) == ["A2", "A3"]

        assert result.metadata.insert_count == 3
