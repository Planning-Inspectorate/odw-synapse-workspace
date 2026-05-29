from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import Row
from odw.test.util.assertion import assert_etl_result_successful, assert_dataframes_equal
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


def _harmonised_appeal_has_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("AppealsHasID", T.LongType(), False),
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
            T.StructField("caseSpecialisms", T.ArrayType(T.StringType(), True), True),
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
            T.StructField("RowID", T.StringType(), False),
            T.StructField("IsActive", T.StringType(), False),
        ]
    )


def _harmonised_appeal_has_row(**overrides):
    base = {
        "caseReference": "HAS-001",
        "AppealsHasID": 1,
        "caseId": "1001",
        "submissionId": None,
        "caseStatus": "submitted",
        "caseType": "HAS",
        "caseProcedure": "WR",
        "lpaCode": "LPA01",
        "caseOfficerId": None,
        "inspectorId": "INS-001",
        "allocationLevel": "Level 1",
        "allocationBand": "Band A",
        "caseSpecialisms": ["Advertisements"],
        "caseSubmittedDate": None,
        "caseCreatedDate": "2025-01-01",
        "caseUpdatedDate": "2025-01-02",
        "caseValidDate": None,
        "caseValidationDate": "2025-01-03",
        "caseValidationOutcome": "Valid appeal",
        "caseValidationInvalidDetails": None,
        "caseValidationIncompleteDetails": None,
        "caseExtensionDate": None,
        "caseStartedDate": None,
        "casePublishedDate": None,
        "linkedCaseStatus": None,
        "leadCaseReference": None,
        "lpaQuestionnaireDueDate": None,
        "lpaQuestionnaireSubmittedDate": None,
        "lpaQuestionnaireCreatedDate": None,
        "lpaQuestionnairePublishedDate": None,
        "lpaQuestionnaireValidationOutcome": None,
        "lpaQuestionnaireValidationOutcomeDate": None,
        "lpaQuestionnaireValidationDetails": None,
        "lpaStatement": None,
        "caseWithdrawnDate": None,
        "caseTransferredDate": None,
        "transferredCaseClosedDate": None,
        "caseDecisionOutcomeDate": None,
        "caseDecisionPublishedDate": None,
        "caseDecisionOutcome": None,
        "caseCompletedDate": None,
        "enforcementNotice": None,
        "applicationReference": None,
        "applicationDate": None,
        "applicationDecision": None,
        "applicationDecisionDate": None,
        "caseSubmissionDueDate": None,
        "siteAddressLine1": "1 Test Street",
        "siteAddressLine2": None,
        "siteAddressTown": "Test Town",
        "siteAddressCounty": None,
        "siteAddressPostcode": "AA1 1AA",
        "siteAccessDetails": None,
        "siteSafetyDetails": None,
        "siteAreaSquareMetres": 1000.0,
        "floorSpaceSquareMetres": 50.0,
        "isCorrectAppealType": None,
        "isGreenBelt": None,
        "inConservationArea": None,
        "ownsAllLand": None,
        "ownsSomeLand": None,
        "knowsOtherOwners": None,
        "knowsAllOwners": None,
        "advertisedAppeal": None,
        "notificationMethod": None,
        "ownersInformed": None,
        "originalDevelopmentDescription": None,
        "changedDevelopmentDescription": None,
        "newConditionDetails": None,
        "nearbyCaseReferences": None,
        "neighbouringSiteAddresses": None,
        "affectedListedBuildingNumbers": None,
        "appellantCostsAppliedFor": None,
        "lpaCostsAppliedFor": None,
        "typeOfPlanningApplication": None,
        "reasonForNeighbourVisits": None,
        "affectsScheduledMonument": None,
        "caseworkReason": None,
        "dateCostsReportDespatched": None,
        "dateNotRecoveredOrDerecovered": None,
        "dateRecovered": None,
        "designatedSitesNames": None,
        "didAppellantSubmitCompletePhotosAndPlans": None,
        "hasInfrastructureLevy": None,
        "hasLandownersPermission": None,
        "hasProtectedSpecies": None,
        "importantInformation": None,
        "infrastructureLevyAdoptedDate": None,
        "infrastructureLevyExpectedDate": None,
        "isAonbNationalLandscape": None,
        "isInfrastructureLevyFormallyAdopted": None,
        "isSiteInAreaOfSpecialControlAdverts": None,
        "advertDetails": None,
        "jurisdiction": None,
        "lpaProcedurePreference": None,
        "lpaProcedurePreferenceDetails": None,
        "lpaProcedurePreferenceDuration": None,
        "originalCaseDecisionDate": None,
        "redeterminedIndicator": None,
        "siteGridReferenceEasting": None,
        "siteGridReferenceNorthing": None,
        "targetDate": None,
        "wasApplicationRefusedDueToHighwayOrTraffic": None,
        "padsSapId": None,
        "migrated": "0",
        "ODTSourceSystem": "ODT",
        "IngestionDate": datetime(2025, 1, 1, 0, 0),
        "message_id": "msg-001",
        "SourceSystemID": "source-001",
        "ValidTo": datetime(2025, 2, 1, 0, 0),
        "RowID": "",
        "IsActive": "N",
    }
    return base | overrides


def _resolver_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("currentGroup", T.StringType(), False),
            T.StructField("asOfTimestamp", T.TimestampType(), True),
        ]
    )


def _resolver_row(**overrides):
    base = {"caseReference": "HAS-001", "currentGroup": "A", "asOfTimestamp": datetime(2025, 2, 1, 0, 0)}
    return base | overrides


class TestRefAppealHasHarmonisationProcess(ETLTestCase):
    def assert_successful_harmonisation(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()
        appeal_has_table = f"{test_case}_appeal_has"

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
        service_bus_table = f"{test_case}_sb_appeal_has"
        self.write_existing_table(spark, service_bus_data, service_bus_table, "odw_harmonised_db", "odw-harmonised", service_bus_table, "overwrite")

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
        horizon_table = f"{test_case}_horizon_appeal_has"
        self.write_existing_table(spark, horizon_data, horizon_table, "odw_standardised_db", "odw-standardised", horizon_table, "overwrite")

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-002", datetime(2025, 4, 1), None, "Y"),
            ],
        )
        appeal_s78_table = f"{test_case}_appeal_s78"
        self.write_existing_table(spark, appeal_s78_data, appeal_s78_table, "odw_harmonised_db", "odw-harmonised", appeal_s78_table, "overwrite")
        expected_appeal_has_data = spark.createDataFrame(
            (
                _harmonised_appeal_has_row(),
                _harmonised_appeal_has_row(
                    AppealsHasID=2, caseStatus="valid", IngestionDate=datetime(2025, 2, 1, 0, 0), message_id="msg-002", ValidTo=None, IsActive="Y"
                ),
                _harmonised_appeal_has_row(
                    caseReference="HAS-002",
                    caseId="1002",
                    caseStatus="horizon submitted",
                    caseProcedure="LI",
                    lpaCode="LPA02",
                    inspectorId="INS-002",
                    allocationLevel="Level 2",
                    allocationBand="Band B",
                    caseSpecialisms=["Listed Building", "Advertisements"],
                    caseCreatedDate="2025-03-01",
                    caseUpdatedDate="2025-03-02",
                    caseValidationDate="2025-03-03",
                    caseStartedDate="2025-03-04",
                    linkedCaseStatus="Linked",
                    leadCaseReference="LEAD-002",
                    lpaQuestionnaireDueDate="2025-03-05",
                    lpaQuestionnaireSubmittedDate="2025-03-06",
                    caseWithdrawnDate="2025-03-07",
                    caseDecisionOutcomeDate="2025-03-08",
                    caseDecisionOutcome="Allowed",
                    caseCompletedDate="2025-03-09",
                    applicationReference="APP-REF-002",
                    applicationDate="2025-03-10",
                    applicationDecisionDate="2025-03-11",
                    siteAddressLine1="Line 1",
                    siteAddressLine2="Line 2",
                    siteAddressTown="Town",
                    siteAddressCounty="County",
                    siteAddressPostcode="BB1 1BB",
                    siteAreaSquareMetres=2000.0,
                    floorSpaceSquareMetres=75.0,
                    isGreenBelt="Y",
                    inConservationArea="N",
                    originalDevelopmentDescription="Development 2",
                    typeOfPlanningApplication="Full Planning",
                    dateCostsReportDespatched="2025-03-12",
                    dateNotRecoveredOrDerecovered="2025-03-13",
                    dateRecovered="2025-03-14",
                    importantInformation="Important horizon info",
                    isAonbNationalLandscape="Y",
                    jurisdiction="HAS",
                    lpaProcedurePreference="Inquiry",
                    originalCaseDecisionDate="2025-03-15",
                    redeterminedIndicator="N",
                    siteGridReferenceEasting="123",
                    siteGridReferenceNorthing="456",
                    migrated=None,
                    ODTSourceSystem="HORIZON",
                    IngestionDate=datetime(2025, 3, 1, 0, 0),
                    message_id=None,
                    SourceSystemID=None,
                    ValidTo=datetime(2025, 4, 1, 0, 0),
                ),
            ),
            schema=_harmonised_appeal_has_schema(),
        )
        expected_group_resolver_data = spark.createDataFrame((_resolver_row(),), schema=_resolver_schema())
        group_resolver_table = f"{test_case}_GroupResolver"
        expected_s78_data_after = _s78_df(
            spark,
            [
                ("HAS-002", datetime(2025, 4, 1), None, "Y"),
            ],
        )
        with (
            mock.patch.object(AppealHasHarmonisationProcess, "OUTPUT_TABLE", appeal_has_table),
            mock.patch.object(AppealHasHarmonisationProcess, "SERVICE_BUS_TABLE", service_bus_table),
            mock.patch.object(AppealHasHarmonisationProcess, "HORIZON_TABLE", horizon_table),
            mock.patch.object(AppealHasHarmonisationProcess, "GROUP_RESOLVER_TABLE", group_resolver_table),
            mock.patch.object(AppealHasHarmonisationProcess, "S78_TABLE", appeal_s78_table),
        ):
            inst = AppealHasHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="ref_appeal_has", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)
            actual_has_data = spark.table(appeal_has_table)
            assert_dataframes_equal(expected_appeal_has_data, actual_has_data)
            actual_group_resolver_data = spark.table(f"odw_harmonised_db.{group_resolver_table}")
            assert_dataframes_equal(expected_group_resolver_data, actual_group_resolver_data)
            actual_s78_data = spark.table(f"odw_harmonised_db.{appeal_s78_table}")
            assert_dataframes_equal(expected_s78_data_after, actual_s78_data)

    def test__appeal_has_harmonisation_process__run__with_no_existing_data(self):
        """
        - Given I have some existing standardised appeal has data, some s78 and service bus/horizon appeal has data
        - When I call AppealHasHarmonisationProcess.run
        - Then the appeal has data should be harmonised, and group resolver/s78 tables updated
        """
        self.assert_successful_harmonisation("t_ahhp_r_wned")

    def test__appeal_has_harmonisation_process__run__with_existing_data(self):
        """
        - Given I have some existing standardised appeal has data, some s78 and service bus/horizon appeal has data and an existing harmonised has table
        - When I call AppealHasHarmonisationProcess.run
        - Then the harmonised appeal has data should be overwritten, and group resolver/s78 tables updated
        """
        test_case = "t_ahhp_r_wed"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_appeal_has = spark.createDataFrame(
            (_harmonised_appeal_has_row(caseReference="some old ref to be deleted"),), schema=_harmonised_appeal_has_schema()
        )
        appeal_has_table = f"{test_case}_appeal_has"
        self.write_existing_table(spark, existing_appeal_has, appeal_has_table, "odw_harmonised_db", "odw-harmonised", appeal_has_table, "overwrite")
        self.assert_successful_harmonisation("t_ahhp_r_wned")
