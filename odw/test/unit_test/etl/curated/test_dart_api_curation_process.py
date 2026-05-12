import mock
from pyspark.sql.types import StringType, StructField, StructType, LongType, DoubleType, ArrayType, BooleanType, TimestampType
from pyspark.sql import DataFrame
from odw.core.etl.transformation.curated.dart_api_curation_process import DartApiCuratedProcess
from odw.core.etl.etl_result import ETLSuccessResult
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
from datetime import datetime
from typing import Callable, List
#import pytest


#pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def generate_harmonised_appeal_has_row(**overrides):
    base = {
        "caseReference": "6000001",
        "caseId": None,
        "submissionId": "",
        "caseStatus": "validation",
        "caseType": "D",
        "caseProcedure": "written",
        "lpaCode": "LOAD1",
        "caseOfficerId": "",
        "inspectorId": "",
        "allocationLevel": "",
        "allocationBand": 0.0,
        "caseSpecialisms": ["specialismA"],
        "caseSubmittedDate": "",
        "caseCreatedDate": "",
        "caseUpdatedDate": "",
        "caseValidDate": "",
        "caseValidationDate": "",
        "caseValidationOutcome": "",
        "caseValidationInvalidDetails": ["caseValidationInvalidDetails"],
        "caseValidationIncompleteDetails": ["caseValidationIncompleteDetails"],
        "caseExtensionDate": "",
        "caseStartedDate": "",
        "casePublishedDate": "",
        "linkedCaseStatus": "",
        "leadCaseReference": "",
        "lpaQuestionnaireDueDate": "",
        "lpaQuestionnaireSubmittedDate": "",
        "lpaQuestionnaireCreatedDate": "",
        "lpaQuestionnairePublishedDate": "",
        "lpaQuestionnaireValidationOutcome": "",
        "lpaQuestionnaireValidationOutcomeDate": "",
        "lpaQuestionnaireValidationDetails": ["lpaQuestionnaireValidationDetails"],
        "lpaStatement": "",
        "caseWithdrawnDate": "",
        "caseTransferredDate": "",
        "transferredCaseClosedDate": "",
        "caseDecisionOutcomeDate": "",
        "caseDecisionPublishedDate": "",
        "caseDecisionOutcome": "",
        "caseCompletedDate": "",
        "enforcementNotice": True,
        "applicationReference": "",
        "applicationDate": "",
        "applicationDecision": "refused",
        "applicationDecisionDate": "",
        "caseSubmissionDueDate": "",
        "siteAddressLine1": "",
        "siteAddressLine2": "",
        "siteAddressTown": "",
        "siteAddressCounty": "",
        "siteAddressPostcode": "",
        "siteAccessDetails": ["siteAccessDetails"],
        "siteSafetyDetails": ["siteSafetyDetails"],
        "siteAreaSquareMetres": 10.0,
        "floorSpaceSquareMetres": 5.0,
        "isCorrectAppealType": True,
        "isGreenBelt": True,
        "inConservationArea": True,
        "affectsScheduledMonument": True,
        "hasProtectedSpecies": True,
        "isAonbNationalLandscape": True,
        "isSiteOnHighwayLand": True,
        "designatedSitesNames": ["designatedSitesNames"],
        "hasInfrastructureLevy": True,
        "isInfrastructureLevyFormallyAdopted": True,
        "infrastructureLevyAdoptedDate": "",
        "infrastructureLevyExpectedDate": "",
        "lpaProcedurePreference": "",
        "lpaProcedurePreferenceDetails": "",
        "lpaProcedurePreferenceDuration": 0.0,
        "caseworkReason": "",
        "importantInformation": "",
        "jurisdiction": "",
        "redeterminedIndicator": True,
        "dateCostsReportDespatched": "",
        "dateNotRecoveredOrDerecovered": "",
        "dateRecovered": "",
        "originalCaseDecisionDate": "",
        "targetDate": "",
        "ownsAllLand": True,
        "ownsSomeLand": True,
        "knowsOtherOwners": "",
        "knowsAllOwners": "",
        "advertisedAppeal": True,
        "notificationMethod": ["notificationMethod"],
        "ownersInformed": True,
        "originalDevelopmentDescription": "",
        "changedDevelopmentDescription": True,
        "newConditionDetails": "",
        "nearbyCaseReferences": ["nearbyCaseReferences"],
        "neighbouringSiteAddresses": [
            {
                "neighbouringSiteAddressLine1": "",
                "neighbouringSiteAddressLine2": "",
                "neighbouringSiteAddressTown": "",
                "neighbouringSiteAddressCounty": "",
                "neighbouringSiteAddressPostcode": "",
                "neighbouringSiteAccessDetails": "",
                "neighbouringSiteSafetyDetails": "",
            }
        ],
        "reasonForNeighbourVisits": "",
        "affectedListedBuildingNumbers": ["affectedListedBuildingNumbers"],
        "appellantCostsAppliedFor": True,
        "lpaCostsAppliedFor": True,
        "typeOfPlanningApplication": "householder-planning",
        "siteGridReferenceEasting": "",
        "siteGridReferenceNorthing": "",
        "hasLandownersPermission": True,
        "wasApplicationRefusedDueToHighwayOrTraffic": True,
        "didAppellantSubmitCompletePhotosAndPlans": True,
        "isSiteInAreaOfSpecialControlAdverts": True,
        "advertDetails": [
            {
                "advertType": "",
                "isAdvertInPosition": True,
                "isSiteOnHighwayLand": True,
            }
        ],
        "padsSapId": "",
        "message_id": "",
        "applicationDecisionDueDate": "",
        "SourceSystemID": "5",
        "RowID": "",
        "migrated": "1",
        "ODTSourceSystem": "ODT",
        "ValidTo": "",
        "IsActive": "Y",
        "AppealsHasID": 1,
        "IngestionDate": datetime(2025, 1, 1),
        "appellantProcedurePreference": "",
        "appellantProcedurePreferenceDetails": "",
        "appellantProcedurePreferenceDuration": "",
        "appellantProcedurePreferenceWitnessCount": "",
        "consultedBodiesDetails": "",
        "designAccessStatementProvided": "",
        "developmentType": "",
        "hasEmergingPlan": True,
        "hasStatutoryConsultees": True,
        "numberOfResidencesNetChange": "",
        "planningObligation": "",
        "siteViewableFromRoad": "",
        "siteWithinSSSI": "",
        "statusPlanningObligation": "",
    }
    return base | overrides


def generate_harmonised_appeal_has_schema():
    return StructType(
        [
            StructField("caseReference", StringType(), True),
            StructField("caseId", LongType(), True),
            StructField("submissionId", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("caseOfficerId", StringType(), True),
            StructField("inspectorId", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", DoubleType(), True),
            StructField("caseSpecialisms", ArrayType(StringType(), True), True),
            StructField("caseSubmittedDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseUpdatedDate", StringType(), True),
            StructField("caseValidDate", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationInvalidDetails", ArrayType(StringType(), True), True),
            StructField("caseValidationIncompleteDetails", ArrayType(StringType(), True), True),
            StructField("caseExtensionDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("casePublishedDate", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("lpaQuestionnaireDueDate", StringType(), True),
            StructField("lpaQuestionnaireSubmittedDate", StringType(), True),
            StructField("lpaQuestionnaireCreatedDate", StringType(), True),
            StructField("lpaQuestionnairePublishedDate", StringType(), True),
            StructField("lpaQuestionnaireValidationOutcome", StringType(), True),
            StructField("lpaQuestionnaireValidationOutcomeDate", StringType(), True),
            StructField("lpaQuestionnaireValidationDetails", ArrayType(StringType(), True), True),
            StructField("lpaStatement", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseTransferredDate", StringType(), True),
            StructField("transferredCaseClosedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseDecisionPublishedDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("enforcementNotice", BooleanType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecision", StringType(), True),
            StructField("applicationDecisionDate", StringType(), True),
            StructField("caseSubmissionDueDate", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("siteAccessDetails", ArrayType(StringType(), True), True),
            StructField("siteSafetyDetails", ArrayType(StringType(), True), True),
            StructField("siteAreaSquareMetres", DoubleType(), True),
            StructField("floorSpaceSquareMetres", DoubleType(), True),
            StructField("isCorrectAppealType", BooleanType(), True),
            StructField("isGreenBelt", BooleanType(), True),
            StructField("inConservationArea", BooleanType(), True),
            StructField("affectsScheduledMonument", BooleanType(), True),
            StructField("hasProtectedSpecies", BooleanType(), True),
            StructField("isAonbNationalLandscape", BooleanType(), True),
            StructField("isSiteOnHighwayLand", BooleanType(), True),
            StructField("designatedSitesNames", ArrayType(StringType(), True), True),
            StructField("hasInfrastructureLevy", BooleanType(), True),
            StructField("isInfrastructureLevyFormallyAdopted", BooleanType(), True),
            StructField("infrastructureLevyAdoptedDate", StringType(), True),
            StructField("infrastructureLevyExpectedDate", StringType(), True),
            StructField("lpaProcedurePreference", StringType(), True),
            StructField("lpaProcedurePreferenceDetails", StringType(), True),
            StructField("lpaProcedurePreferenceDuration", DoubleType(), True),
            StructField("caseworkReason", StringType(), True),
            StructField("importantInformation", StringType(), True),
            StructField("jurisdiction", StringType(), True),
            StructField("redeterminedIndicator", BooleanType(), True),
            StructField("dateCostsReportDespatched", StringType(), True),
            StructField("dateNotRecoveredOrDerecovered", StringType(), True),
            StructField("dateRecovered", StringType(), True),
            StructField("originalCaseDecisionDate", StringType(), True),
            StructField("targetDate", StringType(), True),
            StructField("ownsAllLand", BooleanType(), True),
            StructField("ownsSomeLand", BooleanType(), True),
            StructField("knowsOtherOwners", StringType(), True),
            StructField("knowsAllOwners", StringType(), True),
            StructField("advertisedAppeal", BooleanType(), True),
            StructField("notificationMethod", ArrayType(StringType(), True), True),
            StructField("ownersInformed", BooleanType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("changedDevelopmentDescription", BooleanType(), True),
            StructField("newConditionDetails", StringType(), True),
            StructField("nearbyCaseReferences", ArrayType(StringType(), True), True),
            StructField(
                "neighbouringSiteAddresses",
                ArrayType(
                    StructType(
                        [
                            StructField("neighbouringSiteAddressLine1", StringType(), True),
                            StructField("neighbouringSiteAddressLine2", StringType(), True),
                            StructField("neighbouringSiteAddressTown", StringType(), True),
                            StructField("neighbouringSiteAddressCounty", StringType(), True),
                            StructField("neighbouringSiteAddressPostcode", StringType(), True),
                            StructField("neighbouringSiteAccessDetails", StringType(), True),
                            StructField("neighbouringSiteSafetyDetails", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ),
            StructField("reasonForNeighbourVisits", StringType(), True),
            StructField("affectedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("appellantCostsAppliedFor", BooleanType(), True),
            StructField("lpaCostsAppliedFor", BooleanType(), True),
            StructField("typeOfPlanningApplication", StringType(), True),
            StructField("siteGridReferenceEasting", StringType(), True),
            StructField("siteGridReferenceNorthing", StringType(), True),
            StructField("hasLandownersPermission", BooleanType(), True),
            StructField("wasApplicationRefusedDueToHighwayOrTraffic", BooleanType(), True),
            StructField("didAppellantSubmitCompletePhotosAndPlans", BooleanType(), True),
            StructField("isSiteInAreaOfSpecialControlAdverts", BooleanType(), True),
            StructField(
                "advertDetails",
                ArrayType(
                    StructType(
                        [
                            StructField("advertType", StringType(), True),
                            StructField("isAdvertInPosition", BooleanType(), True),
                            StructField("isSiteOnHighwayLand", BooleanType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("padsSapId", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("applicationDecisionDueDate", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("AppealsHasID", LongType(), True),
            StructField("IngestionDate", TimestampType(), True),
            StructField("appellantProcedurePreference", StringType(), True),
            StructField("appellantProcedurePreferenceDetails", StringType(), True),
            StructField("appellantProcedurePreferenceDuration", StringType(), True),
            StructField("appellantProcedurePreferenceWitnessCount", StringType(), True),
            StructField("consultedBodiesDetails", StringType(), True),
            StructField("designAccessStatementProvided", StringType(), True),
            StructField("developmentType", StringType(), True),
            StructField("hasEmergingPlan", BooleanType(), True),
            StructField("hasStatutoryConsultees", BooleanType(), True),
            StructField("numberOfResidencesNetChange", StringType(), True),
            StructField("planningObligation", StringType(), True),
            StructField("siteViewableFromRoad", StringType(), True),
            StructField("siteWithinSSSI", StringType(), True),
            StructField("statusPlanningObligation", StringType(), True),
        ]
    )


def generate_harmonised_pins_lpa_row(**overrides):
    base = {
        "LPAId": None,
        "lpaName": None,
        "organisationType": None,
        "pinsLpaCode": None,
        "poBox": None,
        "address1": None,
        "address2": None,
        "city": None,
        "county": None,
        "postcode": None,
        "country": None,
        "telephoneNumber": None,
        "fax": None,
        "emailAddress": None,
        "migrated": None,
        "ODTSourceSystem": None,
        "SourceSystemID": None,
        "IngestionDate": datetime(2025, 1, 1),
        "ValidTo": None,
        "RowID": None,
        "IsActive": None,
    }
    return base | overrides


def generate_harmonised_pins_lpa_schema():
    return StructType(
        [
            StructField("LPAId", LongType(), True),
            StructField("lpaName", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("pinsLpaCode", StringType(), True),
            StructField("poBox", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("county", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("country", StringType(), True),
            StructField("telephoneNumber", StringType(), True),
            StructField("fax", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def generate_harmonised_sb_service_user_row(**overrides):
    base = {
        "id": None,
        "salutation": None,
        "firstName": None,
        "lastName": None,
        "addressLine1": None,
        "addressLine2": None,
        "addressTown": None,
        "addressCounty": None,
        "postcode": None,
        "addressCountry": None,
        "organisation": None,
        "organisationType": None,
        "role": None,
        "telephoneNumber": None,
        "otherPhoneNumber": None,
        "faxNumber": None,
        "emailAddress": None,
        "webAddress": None,
        "serviceUserType": None,
        "caseReference": None,
        "sourceSystem": None,
        "sourceSuid": None,
        "migrated": None,
        "ODTSourceSystem": None,
        "SourceSystemID": None,
        "IngestionDate": None,
        "ValidTo": None,
        "RowID": None,
        "IsActive": None,
        "message_id": None,
        "ServiceUserID": 1,
    }
    return base | overrides


def generate_harmonised_sb_service_user_schema():
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
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("ServiceUserID", LongType(), True),
        ]
    )


def generate_harmonised_sb_appeal_event_row(**overrides):
    base = {
        "eventId": None,
        "caseReference": None,
        "eventType": None,
        "eventName": None,
        "eventStatus": None,
        "isUrgent": True,
        "eventPublished": True,
        "eventStartDateTime": None,
        "eventEndDateTime": None,
        "notificationOfSiteVisit": None,
        "addressLine1": None,
        "addressLine2": None,
        "addressTown": None,
        "addressCounty": None,
        "addressPostcode": None,
        "ODTSourceSystem": None,
        "SourceSystemID": None,
        "IngestionDate": None,
        "ValidTo": None,
        "RowID": None,
        "IsActive": None,
        "message_id": None,
        "migrated": None,
        "AppealsEventId": 1,
    }
    return base | overrides


def generate_harmonised_sb_appeal_event_schema():
    return StructType(
        [
            StructField("eventId", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("eventType", StringType(), True),
            StructField("eventName", StringType(), True),
            StructField("eventStatus", StringType(), True),
            StructField("isUrgent", BooleanType(), True),
            StructField("eventPublished", BooleanType(), True),
            StructField("eventStartDateTime", StringType(), True),
            StructField("eventEndDateTime", StringType(), True),
            StructField("notificationOfSiteVisit", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("addressPostcode", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("migrated", StringType(), True),
            StructField("AppealsEventId", LongType(), True),
        ]
    )


def generate_harmonised_entraid_row(**overrides):
    base = {
        "EmployeeEntraId": None,
        "employeeId": None,
        "id": None,
        "givenName": None,
        "surname": None,
        "userPrincipalName": None,
        "migrated": None,
        "ODTSourceSystem": None,
        "SourceSystemID": None,
        "IngestionDate": None,
        "ValidTo": None,
        "RowID": None,
        "IsActive": None,
    }
    return base | overrides


def generate_harmonised_entraid_schema():
    return StructType(
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
    )


def generate_harmonised_horizon_pins_inspector_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "horizonId": None,
        "firstName": None,
        "lastName": None,
        "postName": None,
        "organisationName": None,
        "title": None,
        "salutation": None,
        "qualifications": None,
        "email": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
        "IngestionDate": datetime(2025, 1, 1),
        "Migrated": None,
        "ODTSourceSystem": None,
        "ValidTo": datetime(2025, 1, 1),
        "RowID": None,
        "IsActive": None,
        "SourceSystemID": None,
    }
    return base | overrides


def generate_harmonised_horizon_pins_inspector_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("horizonId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("postName", StringType(), True),
            StructField("organisationName", StringType(), True),
            StructField("title", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("qualifications", StringType(), True),
            StructField("email", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
            StructField("IngestionDate", TimestampType(), True),
            StructField("Migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("ValidTo", TimestampType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
        ]
    )


def generate_harmonised_appeal_s78_row(**overrides):
    base = {
        "caseReference": None,
        "AppealS78ID": 1,
        "caseId": None,
        "submissionId": None,
        "caseStatus": None,
        "caseType": None,
        "caseProcedure": None,
        "lpaCode": None,
        "caseOfficerId": None,
        "inspectorId": None,
        "allocationLevel": None,
        "allocationBand": 0.0,
        "caseSpecialisms": [""],
        "caseSubmittedDate": None,
        "caseCreatedDate": None,
        "caseUpdatedDate": None,
        "caseValidDate": None,
        "caseValidationDate": None,
        "caseValidationOutcome": None,
        "caseValidationInvalidDetails": [""],
        "caseValidationIncompleteDetails": [""],
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
        "lpaQuestionnaireValidationDetails": [""],
        "lpaStatement": None,
        "caseWithdrawnDate": None,
        "caseTransferredDate": None,
        "transferredCaseClosedDate": None,
        "caseDecisionOutcomeDate": None,
        "caseDecisionPublishedDate": None,
        "caseDecisionOutcome": None,
        "caseCompletedDate": None,
        "enforcementNotice": True,
        "applicationReference": None,
        "applicationDate": None,
        "applicationDecision": None,
        "applicationDecisionDate": None,
        "applicationDecisionDueDate": None,
        "caseSubmissionDueDate": None,
        "siteAddressLine1": None,
        "siteAddressLine2": None,
        "siteAddressTown": None,
        "siteAddressCounty": None,
        "siteAddressPostcode": None,
        "siteAccessDetails": [""],
        "siteSafetyDetails": [""],
        "siteAreaSquareMetres": 0.0,
        "floorSpaceSquareMetres": 0.0,
        "isCorrectAppealType": True,
        "isGreenBelt": True,
        "inConservationArea": True,
        "ownsAllLand": True,
        "ownsSomeLand": True,
        "knowsOtherOwners": None,
        "knowsAllOwners": None,
        "advertisedAppeal": True,
        "notificationMethod": [""],
        "ownersInformed": True,
        "originalDevelopmentDescription": None,
        "changedDevelopmentDescription": True,
        "newConditionDetails": None,
        "extraConditionsDetails": None,
        "nearbyCaseReferences": [""],
        "neighbouringSiteAddresses": [
            {
                "neighbouringSiteAddressLine1": None,
                "neighbouringSiteAddressLine2": None,
                "neighbouringSiteAddressTown": None,
                "neighbouringSiteAddressCounty": None,
                "neighbouringSiteAddressPostcode": None,
                "neighbouringSiteAccessDetails": None,
                "neighbouringSiteSafetyDetails": None,
            }
        ],
        "reasonForNeighbourVisits": None,
        "affectedListedBuildingNumbers": [""],
        "changedListedBuildingNumbers": [""],
        "preserveGrantLoan": True,
        "consultHistoricEngland": True,
        "appellantCostsAppliedFor": True,
        "lpaCostsAppliedFor": True,
        "agriculturalHolding": True,
        "tenantAgriculturalHolding": True,
        "otherTenantsAgriculturalHolding": True,
        "informedTenantsAgriculturalHolding": True,
        "appellantProcedurePreference": None,
        "appellantProcedurePreferenceDetails": None,
        "appellantProcedurePreferenceDuration": 0.0,
        "appellantProcedurePreferenceWitnessCount": 0.0,
        "statusPlanningObligation": None,
        "affectsScheduledMonument": True,
        "hasProtectedSpecies": True,
        "isAonbNationalLandscape": True,
        "designatedSitesNames": [""],
        "isGypsyOrTravellerSite": True,
        "isPublicRightOfWay": True,
        "eiaEnvironmentalImpactSchedule": None,
        "eiaDevelopmentDescription": None,
        "eiaSensitiveAreaDetails": None,
        "eiaColumnTwoThreshold": True,
        "eiaScreeningOpinion": True,
        "eiaRequiresEnvironmentalStatement": True,
        "eiaCompletedEnvironmentalStatement": True,
        "hasStatutoryConsultees": True,
        "consultedBodiesDetails": None,
        "hasInfrastructureLevy": True,
        "isInfrastructureLevyFormallyAdopted": True,
        "infrastructureLevyAdoptedDate": None,
        "infrastructureLevyExpectedDate": None,
        "lpaProcedurePreference": None,
        "lpaProcedurePreferenceDetails": None,
        "lpaProcedurePreferenceDuration": 0.0,
        "caseworkReason": None,
        "importantInformation": None,
        "jurisdiction": None,
        "redeterminedIndicator": None,
        "dateCostsReportDespatched": None,
        "dateNotRecoveredOrDerecovered": None,
        "dateRecovered": None,
        "originalCaseDecisionDate": None,
        "targetDate": None,
        "appellantCommentsSubmittedDate": None,
        "appellantStatementDueDate": None,
        "appellantStatementSubmittedDate": None,
        "appellantProofsSubmittedDate": None,
        "finalCommentsDueDate": None,
        "interestedPartyRepsDueDate": None,
        "lpaCommentsSubmittedDate": None,
        "lpaProofsSubmittedDate": None,
        "lpaStatementSubmittedDate": None,
        "proofsOfEvidenceDueDate": None,
        "siteNoticesSentDate": None,
        "statementDueDate": None,
        "caseManagementConferenceDate": None,
        "numberOfResidencesNetChange": 0.0,
        "siteGridReferenceEasting": None,
        "siteGridReferenceNorthing": None,
        "siteViewableFromRoad": True,
        "siteWithinSSSI": True,
        "typeOfPlanningApplication": None,
        "developmentType": None,
        "statementOfCommonGroundDueDate": None,
        "planningObligationDueDate": None,
        "hasLandownersPermission": True,
        "wasApplicationRefusedDueToHighwayOrTraffic": True,
        "didAppellantSubmitCompletePhotosAndPlans": True,
        "isSiteInAreaOfSpecialControlAdverts": True,
        "advertDetails": [
            {
                "advertType": None,
                "isAdvertInPosition": True,
                "isSiteOnHighwayLand": True,
            }
        ],
        "padsSapId": None,
        "enforcementNoticeReference": None,
        "descriptionOfAllegedBreach": None,
        "dateAppellantContactedPins": None,
        "ownerOccupancyStatus": None,
        "occupancyConditionsMet": True,
        "enforcementAppealGroundsDetails": [
            {
                "appealGroundLetter": None,
                "groundForAppealStartDate": None,
                "groundFacts": None,
            }
        ],
        "applicationElbAppealGroundsDetails": [
            {
                "appealGroundLetter": None,
                "groundForAppealStartDate": None,
                "groundFacts": None,
            }
        ],
        "applicationMadeAndFeePaid": True,
        "noticeRelatesToBuildingEngineeringMiningOther": True,
        "changeOfUseRefuseOrWaste": True,
        "changeOfUseMineralExtraction": True,
        "changeOfUseMineralStorage": True,
        "relatesToErectionOfBuildingOrBuildings": True,
        "relatesToBuildingWithAgriculturalPurpose": True,
        "relatesToBuildingSingleDwellingHouse": True,
        "previousPlanningPermissionGranted": True,
        "issueDateOfEnforcementNotice": None,
        "effectiveDateOfEnforcementNotice": None,
        "didAppellantAppealLpaDecision": True,
        "dateLpaDecisionDue": None,
        "dateLpaDecisionReceived": None,
        "areaOfAllegedBreachInSquareMetres": 0.0,
        "doesAllegedBreachCreateFloorSpace": True,
        "floorSpaceCreatedByBreachInSquareMetres": 0.0,
        "applicationPartOrWholeDevelopment": None,
        "hasPcnBeenServed": True,
        "isSiteWithin67MOfTrunkRd": True,
        "affectedTrunkRoadName": None,
        "isSiteOnCrownLand": True,
        "stopNoticeExists": True,
        "isSiteWithin400MOfMineralInterest": True,
        "isSiteWithin250MOfLandfill": True,
        "hasDevelopmentInvolvedWasteImportation": True,
        "isSiteSubjectToArticle4Direction": True,
        "article4AffectedDevelopmentRights": None,
        "pcnRestrictedDevelopmentRights": True,
        "applicationMadeUnderActSection": None,
        "siteUseAtTimeOfApplication": None,
        "appealUnderActSection": None,
        "lpaConsiderAppealInvalid": True,
        "lpaAppealInvalidReasons": None,
        "discontinuanceNoticeServedReason": None,
        "discontinuanceNoticeServedOtherReason": None,
        "wasRetrospectivePlanningApplicationMade": True,
        "reasonForAppealAppellant": None,
        "screeningOpinionIndicatesEiaRequired": True,
        "significantChangesAffectingApplicationAppellant": [{"value": None, "comment": ""}],
        "significantChangesAffectingApplicationLpa": [{"value": None, "comment": ""}],
        "migrated": None,
        "ODTSourceSystem": None,
        "SourceSystemID": None,
        "IngestionDate": datetime(2025, 1, 1),
        "ValidTo": None,
        "RowID": None,
        "IsActive": None,
        "message_id": None,
        "listOfDocumentsBeforeDecision": None,
    }
    return base | overrides


def generate_harmonised_appeal_s78_schema():
    return StructType(
        [
            StructField("caseReference", StringType(), True),
            StructField("AppealS78ID", LongType(), True),
            StructField("caseId", LongType(), True),
            StructField("submissionId", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("caseOfficerId", StringType(), True),
            StructField("inspectorId", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", DoubleType(), True),
            StructField("caseSpecialisms", ArrayType(StringType(), True), True),
            StructField("caseSubmittedDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseUpdatedDate", StringType(), True),
            StructField("caseValidDate", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationInvalidDetails", ArrayType(StringType(), True), True),
            StructField("caseValidationIncompleteDetails", ArrayType(StringType(), True), True),
            StructField("caseExtensionDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("casePublishedDate", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("lpaQuestionnaireDueDate", StringType(), True),
            StructField("lpaQuestionnaireSubmittedDate", StringType(), True),
            StructField("lpaQuestionnaireCreatedDate", StringType(), True),
            StructField("lpaQuestionnairePublishedDate", StringType(), True),
            StructField("lpaQuestionnaireValidationOutcome", StringType(), True),
            StructField("lpaQuestionnaireValidationOutcomeDate", StringType(), True),
            StructField("lpaQuestionnaireValidationDetails", ArrayType(StringType(), True), True),
            StructField("lpaStatement", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseTransferredDate", StringType(), True),
            StructField("transferredCaseClosedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseDecisionPublishedDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("enforcementNotice", BooleanType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecision", StringType(), True),
            StructField("applicationDecisionDate", StringType(), True),
            StructField("applicationDecisionDueDate", StringType(), True),
            StructField("caseSubmissionDueDate", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("siteAccessDetails", ArrayType(StringType(), True), True),
            StructField("siteSafetyDetails", ArrayType(StringType(), True), True),
            StructField("siteAreaSquareMetres", DoubleType(), True),
            StructField("floorSpaceSquareMetres", DoubleType(), True),
            StructField("isCorrectAppealType", BooleanType(), True),
            StructField("isGreenBelt", BooleanType(), True),
            StructField("inConservationArea", BooleanType(), True),
            StructField("ownsAllLand", BooleanType(), True),
            StructField("ownsSomeLand", BooleanType(), True),
            StructField("knowsOtherOwners", StringType(), True),
            StructField("knowsAllOwners", StringType(), True),
            StructField("advertisedAppeal", BooleanType(), True),
            StructField("notificationMethod", ArrayType(StringType(), True), True),
            StructField("ownersInformed", BooleanType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("changedDevelopmentDescription", BooleanType(), True),
            StructField("newConditionDetails", StringType(), True),
            StructField("extraConditionsDetails", StringType(), True),
            StructField("nearbyCaseReferences", ArrayType(StringType(), True), True),
            StructField(
                "neighbouringSiteAddresses",
                ArrayType(
                    StructType(
                        [
                            StructField("neighbouringSiteAddressLine1", StringType(), True),
                            StructField("neighbouringSiteAddressLine2", StringType(), True),
                            StructField("neighbouringSiteAddressTown", StringType(), True),
                            StructField("neighbouringSiteAddressCounty", StringType(), True),
                            StructField("neighbouringSiteAddressPostcode", StringType(), True),
                            StructField("neighbouringSiteAccessDetails", StringType(), True),
                            StructField("neighbouringSiteSafetyDetails", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("reasonForNeighbourVisits", StringType(), True),
            StructField("affectedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("changedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("preserveGrantLoan", BooleanType(), True),
            StructField("consultHistoricEngland", BooleanType(), True),
            StructField("appellantCostsAppliedFor", BooleanType(), True),
            StructField("lpaCostsAppliedFor", BooleanType(), True),
            StructField("agriculturalHolding", BooleanType(), True),
            StructField("tenantAgriculturalHolding", BooleanType(), True),
            StructField("otherTenantsAgriculturalHolding", BooleanType(), True),
            StructField("informedTenantsAgriculturalHolding", BooleanType(), True),
            StructField("appellantProcedurePreference", StringType(), True),
            StructField("appellantProcedurePreferenceDetails", StringType(), True),
            StructField("appellantProcedurePreferenceDuration", DoubleType(), True),
            StructField("appellantProcedurePreferenceWitnessCount", DoubleType(), True),
            StructField("statusPlanningObligation", StringType(), True),
            StructField("affectsScheduledMonument", BooleanType(), True),
            StructField("hasProtectedSpecies", BooleanType(), True),
            StructField("isAonbNationalLandscape", BooleanType(), True),
            StructField("designatedSitesNames", ArrayType(StringType(), True), True),
            StructField("isGypsyOrTravellerSite", BooleanType(), True),
            StructField("isPublicRightOfWay", BooleanType(), True),
            StructField("eiaEnvironmentalImpactSchedule", StringType(), True),
            StructField("eiaDevelopmentDescription", StringType(), True),
            StructField("eiaSensitiveAreaDetails", StringType(), True),
            StructField("eiaColumnTwoThreshold", BooleanType(), True),
            StructField("eiaScreeningOpinion", BooleanType(), True),
            StructField("eiaRequiresEnvironmentalStatement", BooleanType(), True),
            StructField("eiaCompletedEnvironmentalStatement", BooleanType(), True),
            StructField("hasStatutoryConsultees", BooleanType(), True),
            StructField("consultedBodiesDetails", StringType(), True),
            StructField("hasInfrastructureLevy", BooleanType(), True),
            StructField("isInfrastructureLevyFormallyAdopted", BooleanType(), True),
            StructField("infrastructureLevyAdoptedDate", StringType(), True),
            StructField("infrastructureLevyExpectedDate", StringType(), True),
            StructField("lpaProcedurePreference", StringType(), True),
            StructField("lpaProcedurePreferenceDetails", StringType(), True),
            StructField("lpaProcedurePreferenceDuration", DoubleType(), True),
            StructField("caseworkReason", StringType(), True),
            StructField("importantInformation", StringType(), True),
            StructField("jurisdiction", StringType(), True),
            StructField("redeterminedIndicator", StringType(), True),
            StructField("dateCostsReportDespatched", StringType(), True),
            StructField("dateNotRecoveredOrDerecovered", StringType(), True),
            StructField("dateRecovered", StringType(), True),
            StructField("originalCaseDecisionDate", StringType(), True),
            StructField("targetDate", StringType(), True),
            StructField("appellantCommentsSubmittedDate", StringType(), True),
            StructField("appellantStatementDueDate", StringType(), True),
            StructField("appellantStatementSubmittedDate", StringType(), True),
            StructField("appellantProofsSubmittedDate", StringType(), True),
            StructField("finalCommentsDueDate", StringType(), True),
            StructField("interestedPartyRepsDueDate", StringType(), True),
            StructField("lpaCommentsSubmittedDate", StringType(), True),
            StructField("lpaProofsSubmittedDate", StringType(), True),
            StructField("lpaStatementSubmittedDate", StringType(), True),
            StructField("proofsOfEvidenceDueDate", StringType(), True),
            StructField("siteNoticesSentDate", StringType(), True),
            StructField("statementDueDate", StringType(), True),
            StructField("caseManagementConferenceDate", StringType(), True),
            StructField("numberOfResidencesNetChange", DoubleType(), True),
            StructField("siteGridReferenceEasting", StringType(), True),
            StructField("siteGridReferenceNorthing", StringType(), True),
            StructField("siteViewableFromRoad", BooleanType(), True),
            StructField("siteWithinSSSI", BooleanType(), True),
            StructField("typeOfPlanningApplication", StringType(), True),
            StructField("developmentType", StringType(), True),
            StructField("statementOfCommonGroundDueDate", StringType(), True),
            StructField("planningObligationDueDate", StringType(), True),
            StructField("hasLandownersPermission", BooleanType(), True),
            StructField("wasApplicationRefusedDueToHighwayOrTraffic", BooleanType(), True),
            StructField("didAppellantSubmitCompletePhotosAndPlans", BooleanType(), True),
            StructField("isSiteInAreaOfSpecialControlAdverts", BooleanType(), True),
            StructField(
                "advertDetails",
                ArrayType(
                    StructType(
                        [
                            StructField("advertType", StringType(), True),
                            StructField("isAdvertInPosition", BooleanType(), True),
                            StructField("isSiteOnHighwayLand", BooleanType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("padsSapId", StringType(), True),
            StructField("enforcementNoticeReference", StringType(), True),
            StructField("descriptionOfAllegedBreach", StringType(), True),
            StructField("dateAppellantContactedPins", StringType(), True),
            StructField("ownerOccupancyStatus", StringType(), True),
            StructField("occupancyConditionsMet", BooleanType(), True),
            StructField(
                "enforcementAppealGroundsDetails",
                ArrayType(
                    StructType(
                        [
                            StructField("appealGroundLetter", StringType(), True),
                            StructField("groundForAppealStartDate", StringType(), True),
                            StructField("groundFacts", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(
                "applicationElbAppealGroundsDetails",
                ArrayType(
                    StructType(
                        [
                            StructField("appealGroundLetter", StringType(), True),
                            StructField("groundForAppealStartDate", StringType(), True),
                            StructField("groundFacts", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("applicationMadeAndFeePaid", BooleanType(), True),
            StructField("noticeRelatesToBuildingEngineeringMiningOther", BooleanType(), True),
            StructField("changeOfUseRefuseOrWaste", BooleanType(), True),
            StructField("changeOfUseMineralExtraction", BooleanType(), True),
            StructField("changeOfUseMineralStorage", BooleanType(), True),
            StructField("relatesToErectionOfBuildingOrBuildings", BooleanType(), True),
            StructField("relatesToBuildingWithAgriculturalPurpose", BooleanType(), True),
            StructField("relatesToBuildingSingleDwellingHouse", BooleanType(), True),
            StructField("previousPlanningPermissionGranted", BooleanType(), True),
            StructField("issueDateOfEnforcementNotice", StringType(), True),
            StructField("effectiveDateOfEnforcementNotice", StringType(), True),
            StructField("didAppellantAppealLpaDecision", BooleanType(), True),
            StructField("dateLpaDecisionDue", StringType(), True),
            StructField("dateLpaDecisionReceived", StringType(), True),
            StructField("areaOfAllegedBreachInSquareMetres", DoubleType(), True),
            StructField("doesAllegedBreachCreateFloorSpace", BooleanType(), True),
            StructField("floorSpaceCreatedByBreachInSquareMetres", DoubleType(), True),
            StructField("applicationPartOrWholeDevelopment", StringType(), True),
            StructField("hasPcnBeenServed", BooleanType(), True),
            StructField("isSiteWithin67MOfTrunkRd", BooleanType(), True),
            StructField("affectedTrunkRoadName", StringType(), True),
            StructField("isSiteOnCrownLand", BooleanType(), True),
            StructField("stopNoticeExists", BooleanType(), True),
            StructField("isSiteWithin400MOfMineralInterest", BooleanType(), True),
            StructField("isSiteWithin250MOfLandfill", BooleanType(), True),
            StructField("hasDevelopmentInvolvedWasteImportation", BooleanType(), True),
            StructField("isSiteSubjectToArticle4Direction", BooleanType(), True),
            StructField("article4AffectedDevelopmentRights", StringType(), True),
            StructField("pcnRestrictedDevelopmentRights", BooleanType(), True),
            StructField("applicationMadeUnderActSection", StringType(), True),
            StructField("siteUseAtTimeOfApplication", StringType(), True),
            StructField("appealUnderActSection", StringType(), True),
            StructField("lpaConsiderAppealInvalid", BooleanType(), True),
            StructField("lpaAppealInvalidReasons", StringType(), True),
            StructField("discontinuanceNoticeServedReason", StringType(), True),
            StructField("discontinuanceNoticeServedOtherReason", StringType(), True),
            StructField("wasRetrospectivePlanningApplicationMade", BooleanType(), True),
            StructField("reasonForAppealAppellant", StringType(), True),
            StructField("screeningOpinionIndicatesEiaRequired", BooleanType(), True),
            StructField(
                "significantChangesAffectingApplicationAppellant",
                ArrayType(StructType([StructField("value", StringType(), True), StructField("comment", StringType(), True)]), True),
                True,
            ),
            StructField(
                "significantChangesAffectingApplicationLpa",
                ArrayType(StructType([StructField("value", StringType(), True), StructField("comment", StringType(), True)]), True),
                True,
            ),
            StructField("migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", TimestampType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("listOfDocumentsBeforeDecision", StringType(), True),
        ]
    )


def generate_aggregate_row(**overrides):
    base = {
        "caseStatus": "validation",
        "caseType": "D",
        "caseProcedure": "written",
        "allocationLevel": "",
        "allocationBand": 0.0,
        "caseSpecialisms": ["specialismA"],
        "caseSubmittedDate": "",
        "caseCreatedDate": "",
        "caseUpdatedDate": "",
        "caseValidDate": "",
        "caseValidationDate": "",
        "caseValidationOutcome": "",
        "caseValidationInvalidDetails": ["caseValidationInvalidDetails"],
        "caseValidationIncompleteDetails": ["caseValidationIncompleteDetails"],
        "caseExtensionDate": "",
        "caseStartedDate": "",
        "casePublishedDate": "",
        "linkedCaseStatus": "",
        "leadCaseReference": "",
        "caseWithdrawnDate": "",
        "caseTransferredDate": "",
        "transferredCaseClosedDate": "",
        "caseDecisionOutcomeDate": "",
        "caseDecisionPublishedDate": "",
        "caseDecisionOutcome": "",
        "caseCompletedDate": "",
        "enforcementNotice": True,
        "applicationReference": "",
        "applicationDate": "",
        "applicationDecision": "refused",
        "lpaDecisionDate": "",
        "caseSubmissionDueDate": "",
        "siteAddressLine1": "",
        "siteAddressLine2": "",
        "siteAddressTown": "",
        "siteAddressCounty": "",
        "siteAddressPostcode": "",
        "isCorrectAppealType": True,
        "originalDevelopmentDescription": "",
        "changedDevelopmentDescription": True,
        "newConditionDetails": "",
        "nearbyCaseReferences": ["nearbyCaseReferences"],
        "neighbouringSiteAddresses": [
            {
                "neighbouringSiteAddressCounty": "",
                "neighbouringSiteAddressLine1": "",
                "neighbouringSiteSafetyDetails": "",
                "neighbouringSiteAccessDetails": "",
                "neighbouringSiteAddressTown": "",
                "neighbouringSiteAddressPostcode": "",
                "neighbouringSiteAddressLine2": "",
            }
        ],
        "affectedListedBuildingNumbers": ["affectedListedBuildingNumbers"],
        "appellantCostsAppliedFor": True,
        "lpaCostsAppliedFor": True,
    }
    return base | overrides


def generate_aggregate_schema():
    return StructType(
        [
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("lpaName", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", DoubleType(), True),
            StructField("caseSpecialisms", ArrayType(StringType(), True), True),
            StructField("caseSubmittedDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseUpdatedDate", StringType(), True),
            StructField("caseValidDate", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationInvalidDetails", ArrayType(StringType(), True), True),
            StructField("caseValidationIncompleteDetails", ArrayType(StringType(), True), True),
            StructField("caseExtensionDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("casePublishedDate", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseTransferredDate", StringType(), True),
            StructField("transferredCaseClosedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseDecisionPublishedDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("enforcementNotice", BooleanType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecision", StringType(), True),
            StructField("lpaDecisionDate", StringType(), True),
            StructField("caseSubmissionDueDate", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("isCorrectAppealType", BooleanType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("changedDevelopmentDescription", BooleanType(), True),
            StructField("newConditionDetails", StringType(), True),
            StructField("nearbyCaseReferences", ArrayType(StringType(), True), True),
            StructField(
                "neighbouringSiteAddresses",
                ArrayType(
                    StructType(
                        [
                            StructField("neighbouringSiteAddressLine1", StringType(), True),
                            StructField("neighbouringSiteAddressLine2", StringType(), True),
                            StructField("neighbouringSiteAddressTown", StringType(), True),
                            StructField("neighbouringSiteAddressCounty", StringType(), True),
                            StructField("neighbouringSiteAddressPostcode", StringType(), True),
                            StructField("neighbouringSiteAccessDetails", StringType(), True),
                            StructField("neighbouringSiteSafetyDetails", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("affectedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("appellantCostsAppliedFor", BooleanType(), True),
            StructField("lpaCostsAppliedFor", BooleanType(), True),
            StructField("appellantName", StringType(), True),
            StructField("typeOfEvent", StringType(), True),
            StructField("startDateOfTheEvent", StringType(), True),
            StructField("inspectorName", StringType(), True),
            StructField("caseOfficerName", StringType(), True),
            StructField("inspectorQualifications", StringType(), True),
        ]
    )


def generate_aggregate_schema_with_string_boolean_cols():
    return StructType(
        [
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("lpaName", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", DoubleType(), True),
            StructField("caseSpecialisms", ArrayType(StringType(), True), True),
            StructField("caseSubmittedDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseUpdatedDate", StringType(), True),
            StructField("caseValidDate", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationInvalidDetails", ArrayType(StringType(), True), True),
            StructField("caseValidationIncompleteDetails", ArrayType(StringType(), True), True),
            StructField("caseExtensionDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("casePublishedDate", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseTransferredDate", StringType(), True),
            StructField("transferredCaseClosedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseDecisionPublishedDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("enforcementNotice", StringType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecision", StringType(), True),
            StructField("lpaDecisionDate", StringType(), True),
            StructField("caseSubmissionDueDate", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("isCorrectAppealType", StringType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("changedDevelopmentDescription", StringType(), True),
            StructField("newConditionDetails", StringType(), True),
            StructField("nearbyCaseReferences", ArrayType(StringType(), True), True),
            StructField(
                "neighbouringSiteAddresses",
                ArrayType(
                    StructType(
                        [
                            StructField("neighbouringSiteAddressLine1", StringType(), True),
                            StructField("neighbouringSiteAddressLine2", StringType(), True),
                            StructField("neighbouringSiteAddressTown", StringType(), True),
                            StructField("neighbouringSiteAddressCounty", StringType(), True),
                            StructField("neighbouringSiteAddressPostcode", StringType(), True),
                            StructField("neighbouringSiteAccessDetails", StringType(), True),
                            StructField("neighbouringSiteSafetyDetails", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("affectedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("appellantCostsAppliedFor", StringType(), True),
            StructField("lpaCostsAppliedFor", StringType(), True),
            StructField("appellantName", StringType(), True),
            StructField("typeOfEvent", StringType(), True),
            StructField("startDateOfTheEvent", StringType(), True),
            StructField("inspectorName", StringType(), True),
            StructField("caseOfficerName", StringType(), True),
            StructField("inspectorQualifications", StringType(), True),
        ]
    )


def generate_cleaned_row(**overrides):
    base = {
        "caseReference": "1",
        "caseStatus": "validation",
        "caseType": "D",
        "caseProcedure": "written",
        "allocationLevel": "",
        "allocationBand": 0.0,
        "caseSpecialisms": ["specialismA"],
        "caseSubmittedDate": "",
        "caseCreatedDate": "",
        "caseUpdatedDate": "",
        "caseValidDate": "",
        "caseValidationDate": "",
        "caseValidationOutcome": "",
        "caseValidationInvalidDetails": ["caseValidationInvalidDetails"],
        "caseValidationIncompleteDetails": ["caseValidationIncompleteDetails"],
        "caseExtensionDate": "",
        "caseStartedDate": "",
        "casePublishedDate": "",
        "linkedCaseStatus": "",
        "leadCaseReference": "",
        "caseWithdrawnDate": "",
        "caseTransferredDate": "",
        "transferredCaseClosedDate": "",
        "caseDecisionOutcomeDate": "",
        "caseDecisionPublishedDate": "",
        "caseDecisionOutcome": "",
        "caseCompletedDate": "",
        "enforcementNotice": True,
        "applicationReference": "Some Value",
        "applicationDate": "",
        "applicationDecision": "refused",
        "lpaDecisionDate": "",
        "caseSubmissionDueDate": "",
        "siteAddressLine1": "",
        "siteAddressLine2": "",
        "siteAddressTown": "",
        "siteAddressCounty": "",
        "siteAddressPostcode": "",
        "isCorrectAppealType": True,
        "originalDevelopmentDescription": "",
        "changedDevelopmentDescription": True,
        "newConditionDetails": "",
        "nearbyCaseReferences": ["nearbyCaseReferences"],
        "neighbouringSiteAddresses": [
            {
                "neighbouringSiteAddressCounty": "",
                "neighbouringSiteAddressLine1": "",
                "neighbouringSiteSafetyDetails": "",
                "neighbouringSiteAccessDetails": "",
                "neighbouringSiteAddressTown": "",
                "neighbouringSiteAddressPostcode": "",
                "neighbouringSiteAddressLine2": "",
            }
        ],
        "affectedListedBuildingNumbers": ["affectedListedBuildingNumbers"],
        "appellantCostsAppliedFor": True,
        "lpaCostsAppliedFor": True,
    }
    return base | overrides


def generate_cleaned_schema():
    return StructType(
        [
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("lpaName", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", DoubleType(), True),
            StructField("caseSpecialisms", ArrayType(StringType(), True), True),
            StructField("caseSubmittedDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseUpdatedDate", StringType(), True),
            StructField("caseValidDate", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationInvalidDetails", ArrayType(StringType(), True), True),
            StructField("caseValidationIncompleteDetails", ArrayType(StringType(), True), True),
            StructField("caseExtensionDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("casePublishedDate", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseTransferredDate", StringType(), True),
            StructField("transferredCaseClosedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseDecisionPublishedDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("enforcementNotice", BooleanType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecision", StringType(), True),
            StructField("lpaDecisionDate", StringType(), True),
            StructField("caseSubmissionDueDate", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("isCorrectAppealType", BooleanType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("changedDevelopmentDescription", BooleanType(), True),
            StructField("newConditionDetails", StringType(), True),
            StructField("nearbyCaseReferences", ArrayType(StringType(), True), True),
            StructField(
                "neighbouringSiteAddresses",
                ArrayType(
                    StructType(
                        [
                            StructField("neighbouringSiteAddressLine1", StringType(), True),
                            StructField("neighbouringSiteAddressLine2", StringType(), True),
                            StructField("neighbouringSiteAddressTown", StringType(), True),
                            StructField("neighbouringSiteAddressCounty", StringType(), True),
                            StructField("neighbouringSiteAddressPostcode", StringType(), True),
                            StructField("neighbouringSiteAccessDetails", StringType(), True),
                            StructField("neighbouringSiteSafetyDetails", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("affectedListedBuildingNumbers", ArrayType(StringType(), True), True),
            StructField("appellantCostsAppliedFor", BooleanType(), True),
            StructField("lpaCostsAppliedFor", BooleanType(), True),
            StructField("appellantName", StringType(), True),
            StructField("typeOfEvent", StringType(), True),
            StructField("startDateOfTheEvent", StringType(), True),
            StructField("inspectorName", StringType(), True),
            StructField("caseOfficerName", StringType(), True),
            StructField("inspectorQualifications", StringType(), True),
        ]
    )


class TestDartAPICurationProcess(SparkTestCase):
    def test__dart_api_curation_process__load_harmonised_appeal_has(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        kept_rows = [
            generate_harmonised_appeal_has_row(caseId=1),
            generate_harmonised_appeal_has_row(caseId=2),
            generate_harmonised_appeal_has_row(caseId=3),
        ]
        test_case = "t_dacp_lhah"
        appeal_has_data = spark.createDataFrame(
            kept_rows
            + [
                generate_harmonised_appeal_has_row(caseId=4, IsActive="N"),  # Inactive row
                generate_harmonised_appeal_has_row(caseId=5, lpaCode="Q9999"),
                generate_harmonised_appeal_has_row(caseId=6, caseReference=1),
                generate_harmonised_appeal_has_row(caseId=6, ODTSourceSystem="Some other system"),
            ],
            schema=generate_harmonised_appeal_has_schema(),
        )
        self.write_existing_table(spark, appeal_has_data, test_case, "odw_harmonised_db", "odw-harmonised", test_case, "overwrite")
        expected_data = spark.createDataFrame(kept_rows, schema=generate_harmonised_appeal_has_schema())
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, "HARMONISED_APPEAL_HAS", test_case),
        ):
            actual_data = DartAPICurationProcess()._load_harmonised_appeal_has()
            assert_dataframes_equal(expected_data, actual_data)

    def assert_data_load(self, expected_data: DataFrame, target_property: str, test_case_name: str, target_function: Callable):
        spark = PytestSparkSessionUtil().get_spark_session()
        self.write_existing_table(spark, expected_data, test_case_name, "odw_harmonised_db", "odw-harmonised", test_case_name, "overwrite")
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, target_property, test_case_name),
        ):
            inst = DartAPICurationProcess()
            actual_data = target_function(inst)
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__load_harmonised_pins_lpa(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dacp_lhpl"
        expected_data = spark.createDataFrame(
            (
                generate_harmonised_pins_lpa_row(id=1, pinsLpaCode="A"),
                generate_harmonised_pins_lpa_row(id=2, pinsLpaCode="B"),
                generate_harmonised_pins_lpa_row(id=3, pinsLpaCode="C"),
            ),
            schema=generate_harmonised_pins_lpa_schema(),
        )
        self.assert_data_load(expected_data, "HARMONISED_PINS_LPA", test_case, DartAPICurationProcess._load_harmonised_pins_lpa)

    def test__dart_api_curation_process__load_harmonised_sb_service_user(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dacp_lhssu"
        expected_data = spark.createDataFrame(
            (
                generate_harmonised_sb_service_user_row(LPAId=1, firstName="A"),
                generate_harmonised_sb_service_user_row(LPAId=2, firstName="B"),
                generate_harmonised_sb_service_user_row(LPAId=3, firstName="C"),
            ),
            schema=generate_harmonised_sb_service_user_schema(),
        )
        self.assert_data_load(expected_data, "HARMONISED_SB_SERVICE_USER", test_case, DartAPICurationProcess._load_harmonised_sb_service_user)

    def test__dart_api_curation_process__load_harmonised_sb_appeal_event(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dacp_lhsae"
        expected_data = spark.createDataFrame(
            (
                generate_harmonised_sb_appeal_event_row(eventId="1"),
                generate_harmonised_sb_appeal_event_row(eventId="2"),
                generate_harmonised_sb_appeal_event_row(eventId="3"),
            ),
            schema=generate_harmonised_sb_appeal_event_schema(),
        )
        self.assert_data_load(expected_data, "HARMONISED_SB_APPEAL_EVENT", test_case, DartAPICurationProcess._load_harmonised_sb_appeal_event)

    def test__dart_api_curation_process__load_harmonised_entraid(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dacp_lhe"
        expected_data = spark.createDataFrame(
            (
                generate_harmonised_entraid_row(EmployeeEntraId=1, givenName="A"),
                generate_harmonised_entraid_row(EmployeeEntraId=2, givenName="B"),
                generate_harmonised_entraid_row(EmployeeEntraId=3, givenName="C"),
            ),
            schema=generate_harmonised_entraid_schema(),
        )
        self.assert_data_load(expected_data, "HARMONISED_ENTRAID", test_case, DartAPICurationProcess._load_harmonised_entraid)

    def test__dart_api_curation_process__load_harmonised_horizon_pins_inspector(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dacp_lhhpi"
        expected_data = spark.createDataFrame(
            (
                generate_harmonised_horizon_pins_inspector_row(horizonId="1", firstName="A", lastName="D"),
                generate_harmonised_horizon_pins_inspector_row(horizonId="2", firstName="B", lastName="E"),
                generate_harmonised_horizon_pins_inspector_row(horizonId="3", firstName="C", lastName="F"),
            ),
            schema=generate_harmonised_horizon_pins_inspector_schema(),
        )
        self.assert_data_load(expected_data, "HARMONISED_PINS_INSPECTOR", test_case, DartAPICurationProcess._load_harmonised_horizon_pins_inspector)

    # Query 2
    def test__dart_api_curation_process__load_harmonised_appeal_s78(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        kept_rows = [
            generate_harmonised_appeal_s78_row(caseId=1),
            generate_harmonised_appeal_s78_row(caseId=2),
            generate_harmonised_appeal_s78_row(caseId=3),
        ]
        test_case = "t_dacp_lhas78"
        appeal_s78_data = spark.createDataFrame(
            kept_rows
            + [
                generate_harmonised_appeal_s78_row(caseId=4, IsActive="N"),  # Inactive row
                generate_harmonised_appeal_s78_row(caseId=5, lpaCode="Q9999"),
                generate_harmonised_appeal_s78_row(caseId=6, caseReference=1),
                generate_harmonised_appeal_s78_row(caseId=6, ODTSourceSystem="Some other system"),
            ],
            schema=generate_harmonised_appeal_s78_schema(),
        )
        self.write_existing_table(spark, appeal_s78_data, test_case, "odw_harmonised_db", "odw-harmonised", test_case, "overwrite")
        expected_data = spark.createDataFrame(kept_rows, schema=generate_harmonised_appeal_s78_schema())
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, "HARMONISED_APPEAL_S78", test_case),
        ):
            actual_data = DartAPICurationProcess()._load_harmonised_appeal_s78()
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__load_data__calls_transformation_functions(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        mock_appeal_has = spark.createDataFrame([], schema=generate_harmonised_appeal_has_schema())
        mock_appeal_s78 = spark.createDataFrame([], schema=generate_harmonised_appeal_s78_schema())
        mock_pins_lpa = spark.createDataFrame([], schema=generate_harmonised_pins_lpa_schema())
        mock_service_user = spark.createDataFrame([], schema=generate_harmonised_sb_service_user_schema())
        mock_appeal_event = spark.createDataFrame([], schema=generate_harmonised_sb_appeal_event_schema())
        mock_entraid = spark.createDataFrame([], schema=generate_harmonised_entraid_schema())
        mock_pins_inspector = spark.createDataFrame([], schema=generate_harmonised_horizon_pins_inspector_schema())
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_appeal_has", return_value=mock_appeal_has),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_appeal_s78", return_value=mock_appeal_s78),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_pins_lpa", return_value=mock_pins_lpa),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_sb_service_user", return_value=mock_service_user),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_sb_appeal_event", return_value=mock_appeal_event),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_entraid", return_value=mock_entraid),
            mock.patch.object(DartAPICurationProcess, "_load_harmonised_horizon_pins_inspector", return_value=mock_pins_inspector),
        ):
            DartAPICurationProcess().load_data()
            expected_function_calls: List[mock.MagicMock] = [
                DartAPICurationProcess._load_harmonised_appeal_has,
                DartAPICurationProcess._load_harmonised_appeal_s78,
                DartAPICurationProcess._load_harmonised_pins_lpa,
                DartAPICurationProcess._load_harmonised_sb_service_user,
                DartAPICurationProcess._load_harmonised_sb_appeal_event,
                DartAPICurationProcess._load_harmonised_entraid,
                DartAPICurationProcess._load_harmonised_horizon_pins_inspector,
            ]
            uncalled_functions = [x for x in expected_function_calls if not x.called]
            assert not uncalled_functions, f"The following functions were not called by load_data() but were expected: '{uncalled_functions}'"

    def assert_aggregation(self, base_table_row_generator: Callable, base_table_schema_generator: Callable, function_to_test: Callable):
        spark = PytestSparkSessionUtil().get_spark_session()
        # Either appeal has or appeal s78 (which both have the same schema)
        base_table = spark.createDataFrame(
            (
                base_table_row_generator(caseId=1, lpaCode="A", caseReference=1, inspectorId="insId1", caseOfficerId="insId7"),
                base_table_row_generator(caseId=2, lpaCode="B", caseReference=2, inspectorId="insId2", inspcaseOfficerIdectorId="insId6"),
                base_table_row_generator(caseId=3, lpaCode="C", caseReference=3, inspectorId="insId3", caseOfficerId="insId5"),
                base_table_row_generator(
                    caseId=4, lpaCode="D", caseReference=4, inspectorId="insId4", caseOfficerId="insId4"
                ),  # For the dropped pins_lpa row
                base_table_row_generator(
                    caseId=5, lpaCode="E", caseReference=5, inspectorId="insId5", caseOfficerId="insId3"
                ),  # For the dropped service_user row
                base_table_row_generator(
                    caseId=6, lpaCode="F", caseReference=6, inspectorId="insId6", caseOfficerId="insId2"
                ),  # For the dropped service_user row
                base_table_row_generator(
                    caseId=7, lpaCode="G", caseReference=7, inspectorId="insId7", caseOfficerId="insId1"
                ),  # For the dropped appeal_event row
                base_table_row_generator(
                    caseId=8, lpaCode="H", caseReference=8, inspectorId="insId10", caseOfficerId="insId7"
                ),  # For the dropped entraid_ins row
                base_table_row_generator(
                    caseId=9, lpaCode="I", caseReference=9, inspectorId="insId7", caseOfficerId="insId10"
                ),  # For the dropped entraid_co row
            ),
            schema=base_table_schema_generator(),
        )
        pins_lpa = spark.createDataFrame(
            (
                generate_harmonised_pins_lpa_row(id=1, pinsLpaCode="A", caseReference=1),
                generate_harmonised_pins_lpa_row(id=2, pinsLpaCode="B", caseReference=2),
                generate_harmonised_pins_lpa_row(id=3, pinsLpaCode="C", caseReference=3),
                generate_harmonised_pins_lpa_row(id=3, pinsLpaCode="C", caseReference=4, isActive="N"),  # Should be dropped
            ),
            schema=generate_harmonised_pins_lpa_schema(),
        )
        sb_service_user = spark.createDataFrame(
            (
                generate_harmonised_sb_service_user_row(LPAId=1, firstName="A", caseReference=1, serviceUserType="Appellant"),
                generate_harmonised_sb_service_user_row(LPAId=2, firstName="B", caseReference=2, serviceUserType="Appellant"),
                generate_harmonised_sb_service_user_row(LPAId=3, firstName="C", caseReference=3, serviceUserType="Appellant"),
                generate_harmonised_sb_service_user_row(
                    LPAId=4, firstName="D", caseReference=5, serviceUserType="Something else"
                ),  # Should be dropped
                generate_harmonised_sb_service_user_row(LPAId=5, firstName="E", caseReference=6, isActive="N"),  # Should be dropped
            ),
            schema=generate_harmonised_sb_service_user_schema(),
        )
        sb_appeal_event = spark.createDataFrame(
            (
                generate_harmonised_sb_appeal_event_row(eventId="1", caseReference=1),
                generate_harmonised_sb_appeal_event_row(eventId="2", caseReference=2),
                generate_harmonised_sb_appeal_event_row(eventId="3", caseReference=3),
                generate_harmonised_sb_appeal_event_row(eventId="4", caseReference=4),  # Should be dropped
            ),
            schema=generate_harmonised_sb_appeal_event_schema(),
        )
        entraid = spark.createDataFrame(
            (
                generate_harmonised_entraid_row(EmployeeEntraId=1, givenName="A", id="insId1", userPrincipalName="x@pins.gov.uk"),  # ent_ins
                generate_harmonised_entraid_row(EmployeeEntraId=2, givenName="B", id="insId2", userPrincipalName="y@pins.gov.uk"),  # ent_ins
                generate_harmonised_entraid_row(EmployeeEntraId=3, givenName="C", id="insId3", userPrincipalName="z@pins.gov.uk"),  # ent_co
                generate_harmonised_entraid_row(EmployeeEntraId=10, givenName="Z", id="insId10", isActive="N"),  # Should be dropped
            ),
            schema=generate_harmonised_entraid_schema(),
        )
        horizon_pins_inspector = spark.createDataFrame(
            (
                generate_harmonised_horizon_pins_inspector_row(horizonId="1", firstName="A", lastName="D", email="x@pins.gov.uk"),
                generate_harmonised_horizon_pins_inspector_row(horizonId="2", firstName="B", lastName="E", email="y@pins.gov.uk"),
                generate_harmonised_horizon_pins_inspector_row(horizonId="3", firstName="C", lastName="F", email="z@pins.gov.uk"),
                generate_harmonised_horizon_pins_inspector_row(horizonId="4", firstName="D", lastName="G", email="a@pins.gov.uk", isActive="Y"),
            ),
            schema=generate_harmonised_horizon_pins_inspector_schema(),
        )
        expected_data = spark.createDataFrame(
            (
                generate_aggregate_row(
                    caseId=7,
                    caseReference="7",
                    lpaCode="G",
                ),
                generate_aggregate_row(
                    caseId=3,
                    caseReference="3",
                    lpaCode="C",
                ),
                generate_aggregate_row(
                    caseId=8,
                    caseReference="8",
                    lpaCode="H",
                ),
                generate_aggregate_row(
                    caseId=5,
                    caseReference="5",
                    lpaCode="E",
                ),
                generate_aggregate_row(
                    caseId=6,
                    caseReference="6",
                    lpaCode="F",
                ),
                generate_aggregate_row(
                    caseId=9,
                    caseReference="9",
                    lpaCode="I",
                ),
                generate_aggregate_row(
                    caseId=1,
                    caseReference="1",
                    lpaCode="A",
                ),
                generate_aggregate_row(
                    caseId=4,
                    caseReference="4",
                    lpaCode="D",
                ),
                generate_aggregate_row(
                    caseId=2,
                    caseReference="2",
                    lpaCode="B",
                ),
            ),
            schema=generate_aggregate_schema(),
        )
        with mock.patch.object(DartAPICurationProcess, "__init__", return_value=None):
            inst = DartAPICurationProcess()
            actual_data = function_to_test(inst, base_table, pins_lpa, sb_service_user, sb_appeal_event, entraid, horizon_pins_inspector)
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__aggregate_appeal_has_data(self):
        self.assert_aggregation(
            generate_harmonised_appeal_has_row, generate_harmonised_appeal_has_schema, DartAPICurationProcess._aggregate_appeal_has_data
        )

    def test__dart_api_curation_process__aggregate_appeal_s78_data(self):
        self.assert_aggregation(
            generate_harmonised_appeal_s78_row,
            generate_harmonised_appeal_s78_schema,
            DartAPICurationProcess._aggregate_appeal_s78_data,
        )

    def test__dart_api_curation_process__union_has_and_s78_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        aggregate_appeal_has_data = spark.createDataFrame(
            (
                generate_aggregate_row(caseId=1, caseReference="1", lpaCode="A"),
                generate_aggregate_row(caseId=2, caseReference="2", lpaCode="B"),
                generate_aggregate_row(caseId=3, caseReference="3", lpaCode="C"),
            ),
            schema=generate_aggregate_schema(),
        )
        aggregate_appeal_s78_data = spark.createDataFrame(
            (
                generate_aggregate_row(caseId=3, caseReference="3", lpaCode="C"),
                generate_aggregate_row(caseId=4, caseReference="4", lpaCode="D"),
                generate_aggregate_row(caseId=5, caseReference="5", lpaCode="E"),
            ),
            schema=generate_aggregate_schema(),
        )
        expected_data = spark.createDataFrame(
            (
                generate_aggregate_row(caseId=1, caseReference="1", lpaCode="A"),
                generate_aggregate_row(caseId=2, caseReference="2", lpaCode="B"),
                generate_aggregate_row(caseId=3, caseReference="3", lpaCode="C"),
                generate_aggregate_row(caseId=3, caseReference="3", lpaCode="C"),
                generate_aggregate_row(caseId=4, caseReference="4", lpaCode="D"),
                generate_aggregate_row(caseId=5, caseReference="5", lpaCode="E"),
            ),
            schema=generate_aggregate_schema(),
        )
        with mock.patch.object(DartAPICurationProcess, "__init__", return_value=None):
            actual_data = DartAPICurationProcess()._union_has_and_s78_data(aggregate_appeal_has_data, aggregate_appeal_s78_data)
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__clean_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        unioned_data = spark.createDataFrame(
            (
                generate_aggregate_row(
                    caseId=1,
                    caseReference="1",
                    lpaCode="A",
                    applicationReference="Some Value",
                    enforcementNotice=True,  # Should become True
                    isCorrectAppealType=True,  # Should become True
                    appellantCostsAppliedFor=True,  # Should become True
                    lpaCostsAppliedFor=True,  # Should become True
                    changedDevelopmentDescription=True,  # Should become True
                ),
                generate_aggregate_row(
                    caseId=2,
                    caseReference="2",
                    lpaCode="B",
                    applicationReference="",  # Should be replaced with UNKNOWN
                    enforcementNotice=False,  # Should become False
                    isCorrectAppealType=False,  # Should become False
                    appellantCostsAppliedFor=False,  # Should become False
                    lpaCostsAppliedFor=False,  # Should become False
                    changedDevelopmentDescription=False,  # Should become False
                ),
                generate_aggregate_row(
                    caseId=3,
                    caseReference="3",
                    lpaCode="C",
                    applicationReference=None,  # Should be replaced with UNKNOWN
                    enforcementNotice=None,  # Should become None
                    isCorrectAppealType=None,  # Should become None
                    appellantCostsAppliedFor=None,  # Should become None
                    lpaCostsAppliedFor=None,  # Should become TruNonee
                    changedDevelopmentDescription=None,  # Should become None
                ),
                generate_aggregate_row(
                    caseId=4,
                    caseReference="4",
                    lpaCode="D",
                    applicationReference="Some Value with trailing period and space . ",  # ' . ' should be removed from the end of the string
                    enforcementNotice=True,  # Should become True
                    isCorrectAppealType=True,  # Should become True
                    appellantCostsAppliedFor=True,  # Should become True
                    lpaCostsAppliedFor=True,  # Should become True
                    changedDevelopmentDescription=True,  # Should become True
                ),
                generate_aggregate_row(  # Same as first row, but with different applicationReference so should be kept
                    caseReference="1",
                    applicationReference="Some Value 10",
                ),
                generate_aggregate_row(  # Same as first row, but with different caseReference so should be kept
                    caseReference="1000",
                    applicationReference="Some Value 10",
                ),
                generate_aggregate_row(  # Same as first row, so should be dropped
                    caseReference="1",
                    applicationReference="Some Value",
                ),
            ),
            schema=generate_aggregate_schema(),
        )
        expected_data = spark.createDataFrame(
            (
                generate_cleaned_row(
                    caseReference="1",
                    enforcementNotice=True,
                    applicationReference="Some Value",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseReference="1",
                    enforcementNotice=True,
                    applicationReference="Some Value 10",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseReference="1000",
                    enforcementNotice=True,
                    applicationReference="Some Value 10",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=2,
                    caseReference="2",
                    lpaCode="B",
                    enforcementNotice=False,
                    applicationReference="UNKNOWN",
                    isCorrectAppealType=False,
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
                generate_cleaned_row(
                    caseId=3,
                    caseReference="3",
                    lpaCode="C",
                    enforcementNotice=None,
                    applicationReference="UNKNOWN",
                    isCorrectAppealType=None,
                    changedDevelopmentDescription=None,
                    appellantCostsAppliedFor=None,
                    lpaCostsAppliedFor=None,
                ),
                generate_cleaned_row(
                    caseId=4,
                    caseReference="4",
                    lpaCode="D",
                    enforcementNotice=True,
                    applicationReference="Some Value with trailing period and space",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
            ),
            schema=generate_cleaned_schema(),
        )
        with mock.patch.object(DartAPICurationProcess, "__init__", return_value=None):
            actual_data = DartAPICurationProcess()._clean_aggregate_data(unioned_data)
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__clean_data__with_string_boolean_columns(self):
        # This is for apparent legacy logic - the data is currently already boolean so this may not be needed anymore
        spark = PytestSparkSessionUtil().get_spark_session()
        unioned_data = spark.createDataFrame(
            (
                generate_aggregate_row(
                    caseId=1,
                    caseReference="1",
                    lpaCode="A",
                    applicationReference="Some Value",
                    enforcementNotice="Y",  # Should become True
                    isCorrectAppealType="Y",  # Should become True
                    appellantCostsAppliedFor="Y",  # Should become True
                    lpaCostsAppliedFor="Y",  # Should become True
                    changedDevelopmentDescription="Y",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=2,
                    caseReference="2",
                    lpaCode="B",
                    applicationReference="",  # Should be replaced with UNKNOWN
                    enforcementNotice="YES",  # Should become True
                    isCorrectAppealType="YES",  # Should become True
                    appellantCostsAppliedFor="YES",  # Should become True
                    lpaCostsAppliedFor="YES",  # Should become True
                    changedDevelopmentDescription="YES",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=3,
                    caseReference="3",
                    lpaCode="C",
                    applicationReference=None,  # Should be replaced with UNKNOWN
                    enforcementNotice="TRUE",  # Should become True
                    isCorrectAppealType="TRUE",  # Should become True
                    appellantCostsAppliedFor="TRUE",  # Should become True
                    lpaCostsAppliedFor="TRUE",  # Should become True
                    changedDevelopmentDescription="TRUE",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=4,
                    caseReference="4",
                    lpaCode="D",
                    applicationReference="Some Value with trailing period and space . ",  # ' . ' should be removed from the end of the string
                    enforcementNotice="T",  # Should become True
                    isCorrectAppealType="T",  # Should become True
                    appellantCostsAppliedFor="T",  # Should become True
                    lpaCostsAppliedFor="T",  # Should become True
                    changedDevelopmentDescription="T",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=5,
                    caseReference="5",
                    lpaCode="E",
                    applicationReference="Some Value 2",
                    enforcementNotice="1",  # Should become True
                    isCorrectAppealType="1",  # Should become True
                    appellantCostsAppliedFor="1",  # Should become True
                    lpaCostsAppliedFor="1",  # Should become True
                    changedDevelopmentDescription="1",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=6,
                    caseReference="6",
                    lpaCode="F",
                    applicationReference="Some Value 3",
                    enforcementNotice="N",  # Should become False
                    isCorrectAppealType="N",  # Should become True
                    appellantCostsAppliedFor="N",  # Should become True
                    lpaCostsAppliedFor="N",  # Should become True
                    changedDevelopmentDescription="N",  # Should become True
                ),
                generate_aggregate_row(
                    caseId=7,
                    caseReference="7",
                    lpaCode="G",
                    applicationReference="Some Value 4",
                    enforcementNotice="NO",  # Should become False
                    isCorrectAppealType="NO",  # Should become False
                    appellantCostsAppliedFor="NO",  # Should become False
                    lpaCostsAppliedFor="NO",  # Should become False
                    changedDevelopmentDescription="NO",  # Should become False
                ),
                generate_aggregate_row(
                    caseId=8,
                    caseReference="8",
                    lpaCode="H",
                    applicationReference="Some Value 5",
                    enforcementNotice="FALSE",  # Should become False
                    isCorrectAppealType="FALSE",  # Should become False
                    appellantCostsAppliedFor="FALSE",  # Should become False
                    lpaCostsAppliedFor="FALSE",  # Should become False
                    changedDevelopmentDescription="FALSE",  # Should become False
                ),
                generate_aggregate_row(
                    caseId=8,
                    caseReference="8",
                    lpaCode="I",
                    applicationReference="Some Value 6",
                    enforcementNotice="F",  # Should become False
                    isCorrectAppealType="F",  # Should become False
                    appellantCostsAppliedFor="F",  # Should become False
                    lpaCostsAppliedFor="F",  # Should become False
                    changedDevelopmentDescription="F",  # Should become False
                ),
                generate_aggregate_row(
                    caseId=9,
                    caseReference="9",
                    lpaCode="J",
                    applicationReference="Some Value 7",
                    enforcementNotice="0",  # Should become False
                    isCorrectAppealType="0",  # Should become False
                    appellantCostsAppliedFor="0",  # Should become False
                    lpaCostsAppliedFor="0",  # Should become False
                    changedDevelopmentDescription="0",  # Should become False
                ),
                generate_aggregate_row(
                    caseId=10,
                    caseReference="10",
                    lpaCode="K",
                    applicationReference="Some Value 8",
                    enforcementNotice="Some non-boolean value",  # Should become None
                    isCorrectAppealType="Some non-boolean value",  # Should become None
                    appellantCostsAppliedFor="Some non-boolean value",  # Should become None
                    lpaCostsAppliedFor="Some non-boolean value",  # Should become None
                    changedDevelopmentDescription="Some non-boolean value",  # Should become None
                ),
                generate_aggregate_row(
                    caseId=11,
                    caseReference="11",
                    lpaCode="L",
                    applicationReference="Some Value 9",
                    enforcementNotice=None,  # Should become None
                    isCorrectAppealType=None,  # Should become None
                    appellantCostsAppliedFor=None,  # Should become None
                    lpaCostsAppliedFor=None,  # Should become None
                    changedDevelopmentDescription=None,  # Should become None
                ),
                generate_aggregate_row(  # Same as first row, but with different applicationReference so should be kept
                    caseReference="1",
                    applicationReference="Some Value 10",
                ),
                generate_aggregate_row(  # Same as first row, but with different caseReference so should be kept
                    caseReference="1000",
                    applicationReference="Some Value 10",
                ),
                generate_aggregate_row(  # Same as first row, so should be dropped
                    caseReference="1",
                    applicationReference="Some Value",
                ),
            ),
            schema=generate_aggregate_schema_with_string_boolean_cols(),
        )
        expected_data = spark.createDataFrame(
            (
                generate_cleaned_row(
                    caseReference="1",
                    enforcementNotice=True,
                    applicationReference="Some Value",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseReference="1",
                    enforcementNotice=True,
                    applicationReference="Some Value 10",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=10,
                    caseReference="10",
                    lpaCode="K",
                    applicationReference="Some Value 8",
                ),
                generate_cleaned_row(
                    caseReference="1000",
                    enforcementNotice=True,
                    applicationReference="Some Value 10",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=11,
                    caseReference="11",
                    lpaCode="L",
                    applicationReference="Some Value 9",
                ),
                generate_cleaned_row(
                    caseId=2,
                    caseReference="2",
                    lpaCode="B",
                    enforcementNotice=True,
                    applicationReference="UNKNOWN",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=3,
                    caseReference="3",
                    lpaCode="C",
                    enforcementNotice=True,
                    applicationReference="UNKNOWN",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=4,
                    caseReference="4",
                    lpaCode="D",
                    enforcementNotice=True,
                    applicationReference="Some Value with trailing period and space",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=5,
                    caseReference="5",
                    lpaCode="E",
                    enforcementNotice=True,
                    applicationReference="Some Value 2",
                    isCorrectAppealType=True,
                    changedDevelopmentDescription=True,
                    appellantCostsAppliedFor=True,
                    lpaCostsAppliedFor=True,
                ),
                generate_cleaned_row(
                    caseId=6,
                    caseReference="6",
                    lpaCode="F",
                    enforcementNotice=False,
                    applicationReference="Some Value 3",
                    isCorrectAppealType=False,
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
                generate_cleaned_row(
                    caseId=7,
                    caseReference="7",
                    lpaCode="G",
                    enforcementNotice=False,
                    applicationReference="Some Value 4",
                    isCorrectAppealType=False,
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
                generate_cleaned_row(
                    caseId=8,
                    caseReference="8",
                    lpaCode="H",
                    enforcementNotice=False,
                    applicationReference="Some Value 5",
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
                generate_cleaned_row(
                    caseId=8,
                    caseReference="8",
                    lpaCode="I",
                    enforcementNotice=False,
                    applicationReference="Some Value 6",
                    isCorrectAppealType=False,
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
                generate_cleaned_row(
                    caseId=9,
                    caseReference="9",
                    lpaCode="J",
                    enforcementNotice=False,
                    applicationReference="Some Value 7",
                    isCorrectAppealType=False,
                    changedDevelopmentDescription=False,
                    appellantCostsAppliedFor=False,
                    lpaCostsAppliedFor=False,
                ),
            ),
            schema=generate_cleaned_schema(),
        )
        with mock.patch.object(DartAPICurationProcess, "__init__", return_value=None):
            actual_data = DartAPICurationProcess()._clean_aggregate_data(unioned_data)
            assert_dataframes_equal(expected_data, actual_data)

    def test__dart_api_curation_process__process__calls_transformation_functions(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        DartAPICurationProcess._aggregate_appeal_has_data
        DartAPICurationProcess._aggregate_appeal_s78_data
        DartAPICurationProcess._union_has_and_s78_data
        DartAPICurationProcess._clean_aggregate_data
        mock_appeal_has = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_appeal_s78 = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_union = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_cleaned = spark.createDataFrame([], schema=generate_cleaned_schema())
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, "_aggregate_appeal_has_data", return_value=mock_appeal_has),
            mock.patch.object(DartAPICurationProcess, "_aggregate_appeal_s78_data", return_value=mock_appeal_s78),
            mock.patch.object(DartAPICurationProcess, "_union_has_and_s78_data", return_value=mock_union),
            mock.patch.object(DartAPICurationProcess, "_clean_aggregate_data", return_value=mock_cleaned),
        ):
            source_data = dict()
            DartAPICurationProcess().process(source_data)
            expected_function_calls: List[mock.MagicMock] = [
                DartAPICurationProcess._aggregate_appeal_has_data,
                DartAPICurationProcess._aggregate_appeal_s78_data,
                DartAPICurationProcess._union_has_and_s78_data,
                DartAPICurationProcess._clean_aggregate_data,
            ]
            uncalled_functions = [x for x in expected_function_calls if not x.called]
            assert not uncalled_functions, f"The following functions were not called by process() but were expected: '{uncalled_functions}'"

    def test__dart_api_curation_process__process__correct_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        DartAPICurationProcess._aggregate_appeal_has_data
        DartAPICurationProcess._aggregate_appeal_s78_data
        DartAPICurationProcess._union_has_and_s78_data
        DartAPICurationProcess._clean_aggregate_data
        mock_appeal_has = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_appeal_s78 = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_union = spark.createDataFrame([], schema=generate_aggregate_schema())
        mock_cleaned = spark.createDataFrame([], schema=generate_cleaned_schema())
        with (
            mock.patch.object(DartAPICurationProcess, "__init__", return_value=None),
            mock.patch.object(DartAPICurationProcess, "_aggregate_appeal_has_data", return_value=mock_appeal_has),
            mock.patch.object(DartAPICurationProcess, "_aggregate_appeal_s78_data", return_value=mock_appeal_s78),
            mock.patch.object(DartAPICurationProcess, "_union_has_and_s78_data", return_value=mock_union),
            mock.patch.object(DartAPICurationProcess, "_clean_aggregate_data", return_value=mock_cleaned),
        ):
            source_data = dict()
            actual_output = DartAPICurationProcess().process(source_data)
            assert (
                isinstance(actual_output, tuple)
                and len(actual_output) == 2
                and isinstance(actual_output[0], dict)
                and isinstance(actual_output[1], ETLSuccessResult)
            ), "Return value of process should be a tuple containing a dictionary and ETLSucessResult"
