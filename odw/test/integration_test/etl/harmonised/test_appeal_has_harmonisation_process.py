from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import Row
from pyspark.sql import functions as F
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.appeal_has_harmonisation_process import AppealHasHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _appeal_has_schema():
    return T.StructType(
        [
            T.StructField("AppealsHasID", T.LongType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("caseId", T.StringType(), True),
            T.StructField("submissionId", T.StringType(), True),
            T.StructField("caseStatus", T.StringType(), True),
            T.StructField("caseType", T.StringType(), True),
            T.StructField("caseProcedure", T.StringType(), True),
            T.StructField("lpaCode", T.StringType(), True),
            T.StructField("caseOfficerId", T.StringType(), True),
            T.StructField("inspectorId", T.StringType(), True),
            T.StructField("allocationLevel", T.StringType(), True),
            T.StructField("allocationBand", T.StringType(), True),
            T.StructField("caseSpecialisms", T.ArrayType(T.StringType()), True),
            T.StructField("caseSubmittedDate", T.StringType(), True),
            T.StructField("caseCreatedDate", T.StringType(), True),
            T.StructField("caseUpdatedDate", T.StringType(), True),
            T.StructField("caseValidDate", T.StringType(), True),
            T.StructField("caseValidationDate", T.StringType(), True),
            T.StructField("caseValidationOutcome", T.StringType(), True),
            T.StructField("caseValidationInvalidDetails", T.StringType(), True),
            T.StructField("caseValidationIncompleteDetails", T.StringType(), True),
            T.StructField("caseExtensionDate", T.StringType(), True),
            T.StructField("caseStartedDate", T.StringType(), True),
            T.StructField("casePublishedDate", T.StringType(), True),
            T.StructField("linkedCaseStatus", T.StringType(), True),
            T.StructField("leadCaseReference", T.StringType(), True),
            T.StructField("lpaQuestionnaireDueDate", T.StringType(), True),
            T.StructField("lpaQuestionnaireSubmittedDate", T.StringType(), True),
            T.StructField("lpaQuestionnaireCreatedDate", T.StringType(), True),
            T.StructField("lpaQuestionnairePublishedDate", T.StringType(), True),
            T.StructField("lpaQuestionnaireValidationOutcome", T.StringType(), True),
            T.StructField("lpaQuestionnaireValidationOutcomeDate", T.StringType(), True),
            T.StructField("lpaQuestionnaireValidationDetails", T.StringType(), True),
            T.StructField("lpaStatement", T.StringType(), True),
            T.StructField("caseWithdrawnDate", T.StringType(), True),
            T.StructField("caseTransferredDate", T.StringType(), True),
            T.StructField("transferredCaseClosedDate", T.StringType(), True),
            T.StructField("caseDecisionOutcomeDate", T.StringType(), True),
            T.StructField("caseDecisionPublishedDate", T.StringType(), True),
            T.StructField("caseDecisionOutcome", T.StringType(), True),
            T.StructField("caseCompletedDate", T.StringType(), True),
            T.StructField("enforcementNotice", T.StringType(), True),
            T.StructField("applicationReference", T.StringType(), True),
            T.StructField("applicationDate", T.StringType(), True),
            T.StructField("applicationDecision", T.StringType(), True),
            T.StructField("applicationDecisionDate", T.StringType(), True),
            T.StructField("caseSubmissionDueDate", T.StringType(), True),
            T.StructField("siteAddressLine1", T.StringType(), True),
            T.StructField("siteAddressLine2", T.StringType(), True),
            T.StructField("siteAddressTown", T.StringType(), True),
            T.StructField("siteAddressCounty", T.StringType(), True),
            T.StructField("siteAddressPostcode", T.StringType(), True),
            T.StructField("siteAccessDetails", T.StringType(), True),
            T.StructField("siteSafetyDetails", T.StringType(), True),
            T.StructField("siteAreaSquareMetres", T.DoubleType(), True),
            T.StructField("floorSpaceSquareMetres", T.DoubleType(), True),
            T.StructField("isCorrectAppealType", T.StringType(), True),
            T.StructField("isGreenBelt", T.StringType(), True),
            T.StructField("inConservationArea", T.StringType(), True),
            T.StructField("ownsAllLand", T.StringType(), True),
            T.StructField("ownsSomeLand", T.StringType(), True),
            T.StructField("knowsOtherOwners", T.StringType(), True),
            T.StructField("knowsAllOwners", T.StringType(), True),
            T.StructField("advertisedAppeal", T.StringType(), True),
            T.StructField("notificationMethod", T.StringType(), True),
            T.StructField("ownersInformed", T.StringType(), True),
            T.StructField("originalDevelopmentDescription", T.StringType(), True),
            T.StructField("changedDevelopmentDescription", T.StringType(), True),
            T.StructField("newConditionDetails", T.StringType(), True),
            T.StructField("nearbyCaseReferences", T.StringType(), True),
            T.StructField("neighbouringSiteAddresses", T.StringType(), True),
            T.StructField("affectedListedBuildingNumbers", T.StringType(), True),
            T.StructField("appellantCostsAppliedFor", T.StringType(), True),
            T.StructField("lpaCostsAppliedFor", T.StringType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("reasonForNeighbourVisits", T.StringType(), True),
            T.StructField("affectsScheduledMonument", T.StringType(), True),
            T.StructField("caseworkReason", T.StringType(), True),
            T.StructField("dateCostsReportDespatched", T.StringType(), True),
            T.StructField("dateNotRecoveredOrDerecovered", T.StringType(), True),
            T.StructField("dateRecovered", T.StringType(), True),
            T.StructField("designatedSitesNames", T.StringType(), True),
            T.StructField("didAppellantSubmitCompletePhotosAndPlans", T.StringType(), True),
            T.StructField("hasInfrastructureLevy", T.StringType(), True),
            T.StructField("hasLandownersPermission", T.StringType(), True),
            T.StructField("hasProtectedSpecies", T.StringType(), True),
            T.StructField("importantInformation", T.StringType(), True),
            T.StructField("infrastructureLevyAdoptedDate", T.StringType(), True),
            T.StructField("infrastructureLevyExpectedDate", T.StringType(), True),
            T.StructField("isAonbNationalLandscape", T.StringType(), True),
            T.StructField("isInfrastructureLevyFormallyAdopted", T.StringType(), True),
            T.StructField("isSiteInAreaOfSpecialControlAdverts", T.StringType(), True),
            T.StructField("advertDetails", T.StringType(), True),
            T.StructField("jurisdiction", T.StringType(), True),
            T.StructField("lpaProcedurePreference", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDetails", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDuration", T.StringType(), True),
            T.StructField("originalCaseDecisionDate", T.StringType(), True),
            T.StructField("redeterminedIndicator", T.StringType(), True),
            T.StructField("siteGridReferenceEasting", T.StringType(), True),
            T.StructField("siteGridReferenceNorthing", T.StringType(), True),
            T.StructField("targetDate", T.StringType(), True),
            T.StructField("wasApplicationRefusedDueToHighwayOrTraffic", T.StringType(), True),
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("message_id", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _horizon_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("caseId", T.StringType(), True),
            T.StructField("caseStatus", T.StringType(), True),
            T.StructField("caseType", T.StringType(), True),
            T.StructField("caseProcedure", T.StringType(), True),
            T.StructField("lpaCode", T.StringType(), True),
            T.StructField("inspectorId", T.StringType(), True),
            T.StructField("allocationLevel", T.StringType(), True),
            T.StructField("allocationBand", T.StringType(), True),
            T.StructField("caseSpecialisms", T.StringType(), True),
            T.StructField("caseCreatedDate", T.StringType(), True),
            T.StructField("caseUpdatedDate", T.StringType(), True),
            T.StructField("caseValidationDate", T.StringType(), True),
            T.StructField("caseValidationOutcome", T.StringType(), True),
            T.StructField("caseStartedDate", T.StringType(), True),
            T.StructField("linkedCaseStatus", T.StringType(), True),
            T.StructField("leadCaseReference", T.StringType(), True),
            T.StructField("lpaQuestionnaireDueDate", T.StringType(), True),
            T.StructField("lpaQuestionnaireSubmittedDate", T.StringType(), True),
            T.StructField("caseWithdrawnDate", T.StringType(), True),
            T.StructField("caseDecisionOutcomeDate", T.StringType(), True),
            T.StructField("caseDecisionOutcome", T.StringType(), True),
            T.StructField("caseCompletedDate", T.StringType(), True),
            T.StructField("applicationReference", T.StringType(), True),
            T.StructField("applicationDate", T.StringType(), True),
            T.StructField("applicationDecisionDate", T.StringType(), True),
            T.StructField("siteAddressLine1", T.StringType(), True),
            T.StructField("siteAddressLine2", T.StringType(), True),
            T.StructField("siteAddressTown", T.StringType(), True),
            T.StructField("siteAddressCounty", T.StringType(), True),
            T.StructField("siteAddressPostcode", T.StringType(), True),
            T.StructField("siteAreaSquareMetres", T.DoubleType(), True),
            T.StructField("floorSpaceSquareMetres", T.DoubleType(), True),
            T.StructField("isGreenBelt", T.StringType(), True),
            T.StructField("inConservationArea", T.StringType(), True),
            T.StructField("originalDevelopmentDescription", T.StringType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("dateCostsReportDespatched", T.StringType(), True),
            T.StructField("dateNotRecoveredOrDerecovered", T.StringType(), True),
            T.StructField("dateRecovered", T.StringType(), True),
            T.StructField("importantInformation", T.StringType(), True),
            T.StructField("isAonbNationalLandscape", T.StringType(), True),
            T.StructField("jurisdiction", T.StringType(), True),
            T.StructField("lpaProcedurePreference", T.StringType(), True),
            T.StructField("originalCaseDecisionDate", T.StringType(), True),
            T.StructField("redeterminedIndicator", T.StringType(), True),
            T.StructField("siteGridReferenceEasting", T.StringType(), True),
            T.StructField("siteGridReferenceNorthing", T.StringType(), True),
            T.StructField("ingested_datetime", T.TimestampType(), True),
            T.StructField("expected_from", T.TimestampType(), True),
        ]
    )


def _s78_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_df(spark, schema):
    return spark.createDataFrame([], schema)


def _base_sb_row(**overrides):
    row = {field.name: None for field in _appeal_has_schema()}
    row.update(
        {
            "AppealsHasID": None,
            "caseReference": "HAS-001",
            "caseId": "1001",
            "caseStatus": "submitted",
            "caseType": "HAS",
            "caseProcedure": "WR",
            "lpaCode": "LPA01",
            "inspectorId": "INS-001",
            "allocationLevel": "Level 1",
            "allocationBand": "Band A",
            "caseSpecialisms": ["Advertisements"],
            "caseCreatedDate": "2025-01-01",
            "caseUpdatedDate": "2025-01-02",
            "caseValidationDate": "2025-01-03",
            "caseValidationOutcome": "Valid appeal",
            "siteAddressLine1": "1 Test Street",
            "siteAddressTown": "Test Town",
            "siteAddressPostcode": "AA1 1AA",
            "siteAreaSquareMetres": 1000.0,
            "floorSpaceSquareMetres": 50.0,
            "migrated": "0",
            "ODTSourceSystem": "ODT",
            "IngestionDate": datetime(2025, 1, 1),
            "message_id": "msg-001",
            "SourceSystemID": "source-001",
            "ValidTo": None,
            "RowID": "",
            "IsActive": "Y",
        }
    )
    row.update(overrides)
    return Row(**row)


def _sb_df(spark, rows):
    return spark.createDataFrame(rows, _appeal_has_schema())


def _horizon_df(spark, rows):
    return spark.createDataFrame(rows, _horizon_schema())


def _s78_df(spark, rows=None):
    rows = rows or []
    return spark.createDataFrame(rows, _s78_schema())


def _source_data(spark, service_bus_data=None, horizon_data=None, appeal_s78_data=None):
    return {
        "service_bus_data": service_bus_data or _empty_df(spark, _appeal_has_schema()),
        "horizon_data": horizon_data or _empty_df(spark, _horizon_schema()),
        "appeal_s78_data": appeal_s78_data or _s78_df(spark),
    }


class TestRefAppealHasHarmonisationProcess(ETLTestCase):
    def test__appeal_has_harmonisation_process__run__harmonises_service_bus_and_horizon_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="submitted",
                    caseType="HAS",
                    caseProcedure="WR",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 1, 1),
                ),
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="valid",
                    caseType="HAS",
                    caseProcedure="WR",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 2, 1),
                    message_id="msg-002",
                ),
            ],
        )

        horizon_data = _horizon_df(
            spark,
            [
                (
                    "HAS-002",
                    "1002",
                    "horizon submitted",
                    "HAS",
                    "LI",
                    "LPA02",
                    "INS-002",
                    "Level 2",
                    "Band B",
                    "Listed Building,Advertisements",
                    "2025-03-01",
                    "2025-03-02",
                    "2025-03-03",
                    "Valid appeal",
                    "2025-03-04",
                    "Linked",
                    "LEAD-002",
                    "2025-03-05",
                    "2025-03-06",
                    "2025-03-07",
                    "2025-03-08",
                    "Allowed",
                    "2025-03-09",
                    "APP-REF-002",
                    "2025-03-10",
                    "2025-03-11",
                    "Line 1",
                    "Line 2",
                    "Town",
                    "County",
                    "BB1 1BB",
                    2000.0,
                    75.0,
                    "Y",
                    "N",
                    "Development 2",
                    "Full Planning",
                    "2025-03-12",
                    "2025-03-13",
                    "2025-03-14",
                    "Important horizon info",
                    "Y",
                    "HAS",
                    "Inquiry",
                    "2025-03-15",
                    "N",
                    "123",
                    "456",
                    datetime(2025, 3, 1),
                    datetime(2025, 2, 28),
                )
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-002", datetime(2025, 4, 1), None, "Y"),
            ],
        )

        source_data = _source_data(spark, service_bus_data, horizon_data, appeal_s78_data)

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_has_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealHasHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        rows = {(row["caseReference"], row["IngestionDate"]): row for row in df.collect()}

        assert df.count() == 3
        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 3

        assert rows[("HAS-001", datetime(2025, 1, 1))]["IsActive"] == "N"
        assert rows[("HAS-001", datetime(2025, 1, 1))]["ValidTo"] == datetime(2025, 2, 1)
        assert rows[("HAS-001", datetime(2025, 2, 1))]["IsActive"] == "Y"
        assert rows[("HAS-001", datetime(2025, 2, 1))]["ValidTo"] is None

        assert rows[("HAS-002", datetime(2025, 3, 1))]["ODTSourceSystem"] == "HORIZON"
        assert rows[("HAS-002", datetime(2025, 3, 1))]["caseSpecialisms"] == ["Listed Building", "Advertisements"]
        assert rows[("HAS-002", datetime(2025, 3, 1))]["ValidTo"] == datetime(2025, 4, 1)
        assert rows[("HAS-002", datetime(2025, 3, 1))]["IsActive"] == "N"
        assert rows[("HAS-002", datetime(2025, 3, 1))]["caseStatus"] == "horizon submitted"
        assert rows[("HAS-002", datetime(2025, 3, 1))]["siteAddressPostcode"] == "BB1 1BB"

    def test__appeal_has_harmonisation_process__run__empty_sources_write_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(spark)

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_has_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealHasHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["partition_by"] == ["IsActive"]
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "delta"
        assert result.metadata.insert_count == 0

    def test__appeal_has_harmonisation_process__run__covers_scd2_duplicates_ties_s78_and_schema_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="submitted",
                    RowID="a-row",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 1, 1),
                    message_id="msg-001",
                ),
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="valid",
                    RowID="b-row",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 2, 1),
                    message_id="msg-002",
                ),
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="valid",
                    RowID="b-row",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 2, 1),
                    message_id="msg-002",
                ),
                _base_sb_row(
                    caseReference=None,
                    caseStatus="null key should be removed",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 1, 1),
                    message_id="bad-msg",
                ),
            ],
        )

        horizon_data = _horizon_df(
            spark,
            [
                (
                    "HAS-002",
                    "1002",
                    "horizon submitted",
                    "HAS",
                    "LI",
                    "LPA02",
                    "INS-002",
                    "Level 2",
                    "Band B",
                    "Listed Building,Advertisements",
                    "2025-03-01",
                    "2025-03-02",
                    "2025-03-03",
                    "Valid appeal",
                    "2025-03-04",
                    "Linked",
                    "LEAD-002",
                    "2025-03-05",
                    "2025-03-06",
                    "2025-03-07",
                    "2025-03-08",
                    "Allowed",
                    "2025-03-09",
                    "APP-REF-002",
                    "2025-03-10",
                    "2025-03-11",
                    "Line 1",
                    "Line 2",
                    "Town",
                    "County",
                    "BB1 1BB",
                    2000.0,
                    75.0,
                    "Y",
                    "N",
                    "Development 2",
                    "Full Planning",
                    "2025-03-12",
                    "2025-03-13",
                    "2025-03-14",
                    "Important horizon info",
                    "Y",
                    "HAS",
                    "Inquiry",
                    "2025-03-15",
                    "N",
                    "123",
                    "456",
                    None,
                    datetime(2025, 3, 1),
                )
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-002", datetime(2025, 4, 1), None, "Y"),
            ],
        )

        source_data = _source_data(spark, service_bus_data, horizon_data, appeal_s78_data)

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_has_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealHasHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        rows = {(row["caseReference"], row["IngestionDate"]): row for row in df.collect()}

        assert df.count() == 3
        assert df.where(F.col("caseReference").isNull()).count() == 0
        assert df.where(F.col("IsActive") == "Y").groupBy("caseReference").count().where("count > 1").count() == 0

        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert df.columns == [field.name for field in _appeal_has_schema()]
        assert result.metadata.insert_count == 3

        assert rows[("HAS-001", datetime(2025, 1, 1))]["IsActive"] == "N"
        assert rows[("HAS-001", datetime(2025, 1, 1))]["ValidTo"] == datetime(2025, 2, 1)
        assert rows[("HAS-001", datetime(2025, 1, 1))]["AppealsHasID"] == 1

        assert rows[("HAS-001", datetime(2025, 2, 1))]["IsActive"] == "Y"
        assert rows[("HAS-001", datetime(2025, 2, 1))]["ValidTo"] is None
        assert rows[("HAS-001", datetime(2025, 2, 1))]["AppealsHasID"] == 2

        assert rows[("HAS-002", datetime(2025, 3, 1))]["ODTSourceSystem"] == "HORIZON"
        assert rows[("HAS-002", datetime(2025, 3, 1))]["IngestionDate"] == datetime(2025, 3, 1)
        assert rows[("HAS-002", datetime(2025, 3, 1))]["caseSpecialisms"] == ["Listed Building", "Advertisements"]
        assert rows[("HAS-002", datetime(2025, 3, 1))]["ValidTo"] == datetime(2025, 4, 1)
        assert rows[("HAS-002", datetime(2025, 3, 1))]["IsActive"] == "N"
