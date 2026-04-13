import pytest
from odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process import NsipProjectHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
import mock
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _service_bus_schema():
    return StructType(
        [
            StructField("NSIPProjectInfoInternalID", LongType(), True),
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("projectName", StringType(), True),
            StructField("projectNameWelsh", StringType(), True),
            StructField("projectDescription", StringType(), True),
            StructField("projectDescriptionWelsh", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("decision", StringType(), True),
            StructField("publishStatus", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("projectType", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("projectLocation", StringType(), True),
            StructField("projectLocationWelsh", StringType(), True),
            StructField("projectEmailAddress", StringType(), True),
            StructField("regions", ArrayType(StringType()), True),
            StructField("transboundary", BooleanType(), True),
            StructField("easting", IntegerType(), True),
            StructField("northing", IntegerType(), True),
            StructField("welshLanguage", BooleanType(), True),
            StructField("mapZoomLevel", StringType(), True),
            StructField("secretaryOfState", StringType(), True),
            StructField("datePINSFirstNotifiedOfProject", StringType(), True),
            StructField("dateProjectAppearsOnWebsite", StringType(), True),
            StructField("anticipatedSubmissionDateNonSpecific", StringType(), True),
            StructField("anticipatedDateOfSubmission", StringType(), True),
            StructField("screeningOpinionSought", StringType(), True),
            StructField("screeningOpinionIssued", StringType(), True),
            StructField("scopingOpinionSought", StringType(), True),
            StructField("scopingOpinionIssued", StringType(), True),
            StructField("section46Notification", StringType(), True),
            StructField("dateOfDCOSubmission", StringType(), True),
            StructField("deadlineForAcceptanceDecision", StringType(), True),
            StructField("dateOfDCOAcceptance", StringType(), True),
            StructField("dateOfNonAcceptance", StringType(), True),
            StructField("dateOfRepresentationPeriodOpen", StringType(), True),
            StructField("dateOfRelevantRepresentationClose", StringType(), True),
            StructField("extensionToDateRelevantRepresentationsClose", StringType(), True),
            StructField("dateRRepAppearOnWebsite", StringType(), True),
            StructField("dateIAPIDue", StringType(), True),
            StructField("rule6LetterPublishDate", StringType(), True),
            StructField("preliminaryMeetingStartDate", StringType(), True),
            StructField("notificationDateForPMAndEventsDirectlyFollowingPM", StringType(), True),
            StructField("notificationDateForEventsDeveloper", StringType(), True),
            StructField("dateSection58NoticeReceived", StringType(), True),
            StructField("confirmedStartOfExamination", StringType(), True),
            StructField("rule8LetterPublishDate", StringType(), True),
            StructField("deadlineForCloseOfExamination", StringType(), True),
            StructField("dateTimeExaminationEnds", StringType(), True),
            StructField("stage4ExtensionToExamCloseDate", StringType(), True),
            StructField("deadlineForSubmissionOfRecommendation", StringType(), True),
            StructField("dateOfRecommendations", StringType(), True),
            StructField("stage5ExtensionToRecommendationDeadline", StringType(), True),
            StructField("deadlineForDecision", StringType(), True),
            StructField("confirmedDateOfDecision", StringType(), True),
            StructField("stage5ExtensionToDecisionDeadline", StringType(), True),
            StructField("jRPeriodEndDate", StringType(), True),
            StructField("dateProjectWithdrawn", StringType(), True),
            StructField("operationsLeadId", StringType(), True),
            StructField("operationsManagerId", StringType(), True),
            StructField("caseManagerId", StringType(), True),
            StructField("nsipOfficerIds", ArrayType(StringType()), True),
            StructField("nsipAdministrationOfficerIds", ArrayType(StringType()), True),
            StructField("leadInspectorId", StringType(), True),
            StructField("inspectorIds", ArrayType(StringType()), True),
            StructField("environmentalServicesOfficerId", StringType(), True),
            StructField("legalOfficerId", StringType(), True),
            StructField("applicantId", StringType(), True),
            StructField("migrationStatus", BooleanType(), True),
            StructField("dateOfReOpenRelevantRepresentationStart", StringType(), True),
            StructField("dateOfReOpenRelevantRepresentationClose", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("examTimetablePublishStatus", StringType(), True),
            StructField("twitteraccountname", StringType(), True),
            StructField("exasize", StringType(), True),
            StructField("tene", StringType(), True),
            StructField("promotername", StringType(), True),
            StructField("applicantfirstname", StringType(), True),
            StructField("applicantlastname", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("Postcode", StringType(), True),
            StructField("applicantemailaddress", StringType(), True),
            StructField("applicantwebaddress", StringType(), True),
            StructField("applicantphonenumber", StringType(), True),
            StructField("applicantdescriptionofproject", StringType(), True),
            StructField("HorizonCaseNumber", StringType(), True),
            StructField("inceptionMeetingDate", StringType(), True),
            StructField("draftDocumentSubmissionDate", StringType(), True),
            StructField("programmeDocumentSubmissionDate", StringType(), True),
            StructField("estimatedScopingSubmissionDate", StringType(), True),
            StructField("consultationMilestoneAdequacyDate", StringType(), True),
            StructField("principalAreaDisagreementSummaryStmtSubmittedDate", StringType(), True),
            StructField("policyComplianceDocumentSubmittedDate", StringType(), True),
            StructField("designApproachDocumentSubmittedDate", StringType(), True),
            StructField("caAndTpEvidenceSubmittedDate", StringType(), True),
            StructField("caseTeamIssuedCommentsDate", StringType(), True),
            StructField("fastTrackAdmissionDocumentSubmittedDate", StringType(), True),
            StructField("matureOutlineControlDocumentSubmittedDate", StringType(), True),
            StructField("memLastUpdated", StringType(), True),
            StructField("multipartyApplicationCheckDocumentSubmittedDate", StringType(), True),
            StructField("programmeDocumentReviewedByEstDate", StringType(), True),
            StructField("publicSectorEqualityDutySubmittedDate", StringType(), True),
            StructField("statutoryConsultationPeriodEndDate", StringType(), True),
            StructField("submissionOfDraftDocumentsDate", StringType(), True),
            StructField("updatedProgrammeDocumentReceivedDate", StringType(), True),
            StructField("courtDecisionDate", StringType(), True),
            StructField("decisionChallengeSubmissionDate", StringType(), True),
            StructField("courtDecisionOutcomeText", StringType(), True),
            StructField("recommendation", StringType(), True),
            StructField("additionalComments", StringType(), True),
            StructField("caAndTpEvidence", StringType(), True),
            StructField("fastTrackAdmissionDocument", StringType(), True),
            StructField("meetings", StringType(), True),
            StructField("multipartyApplicationCheckDocument", StringType(), True),
            StructField("newMaturity", StringType(), True),
            StructField("numberBand2Inspectors", DoubleType(), True),
            StructField("numberBand3Inspectors", DoubleType(), True),
            StructField("programmeDocumentURI", StringType(), True),
            StructField("publicSectorEqualityDuty", StringType(), True),
            StructField("subProjectType", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("s61SummaryURI", StringType(), True),
            StructField("planProcessEvidence", BooleanType(), True),
            StructField("issuesTracker", StringType(), True),
            StructField("essentialFastTrackComponents", BooleanType(), True),
            StructField("principalAreaDisagreementSummaryStmt", StringType(), True),
            StructField("policyComplianceDocument", StringType(), True),
            StructField("designApproachDocument", StringType(), True),
            StructField("matureOutlineControlDocument", StringType(), True),
            StructField("invoices", StringType(), True),
            StructField("operationsLeadIds", ArrayType(StringType()), True),
            StructField("operationsManagerIds", ArrayType(StringType()), True),
            StructField("caseManagerIds", ArrayType(StringType()), True),
            StructField("leadInspectorIds", ArrayType(StringType()), True),
            StructField("environmentalServicesOfficerIds", ArrayType(StringType()), True),
            StructField("legalOfficerIds", ArrayType(StringType()), True),
            StructField("migrated", BooleanType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("isMaterialChange", BooleanType(), True),
        ]
    )


def _horizon_schema():
    return StructType(
        [
            StructField("caseNodeId", LongType(), True),
            StructField("casereference", StringType(), True),
            StructField("projectname", StringType(), True),
            StructField("projectNameWelsh", StringType(), True),
            StructField("projectDescription", StringType(), True),
            StructField("projectDescriptionWelsh", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("projectstatus", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("projecttype", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("projectLocation", StringType(), True),
            StructField("projectLocationWelsh", StringType(), True),
            StructField("projectEmailAddress", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("transboundary", BooleanType(), True),
            StructField("easting", IntegerType(), True),
            StructField("northing", IntegerType(), True),
            StructField("welshLanguage", BooleanType(), True),
            StructField("mapZoomLevel", StringType(), True),
            StructField("sos", StringType(), True),
            StructField("datePINSFirstNotifiedOfProject", StringType(), True),
            StructField("dateProjectAppearsOnWebsite", StringType(), True),
            StructField("anticipatedSubmissionDateNonSpecific", StringType(), True),
            StructField("anticipatedDateOfSubmission", StringType(), True),
            StructField("screeningOpinionSought", StringType(), True),
            StructField("screeningOpinionIssued", StringType(), True),
            StructField("scopingOpinionSought", StringType(), True),
            StructField("scopingOpinionIssued", StringType(), True),
            StructField("section46Notification", StringType(), True),
            StructField("dateOfDCOSubmission", StringType(), True),
            StructField("deadlineForAcceptanceDecision", StringType(), True),
            StructField("dateOfDCOAcceptance", StringType(), True),
            StructField("dateOfRepresentationPeriodOpen", StringType(), True),
            StructField("dateOfRelevantRepresentationClose", StringType(), True),
            StructField("extensionToDateRelevantRepresentationsClose", StringType(), True),
            StructField("dateRRepAppearOnWebsite", StringType(), True),
            StructField("preliminaryMeetingStartDate", StringType(), True),
            StructField("dateSection58NoticeReceived", StringType(), True),
            StructField("confirmedStartOfExamination", StringType(), True),
            StructField("deadlineForCloseOfExamination", StringType(), True),
            StructField("dateTimeExaminationEnds", StringType(), True),
            StructField("stage4ExtensionToExamCloseDate", StringType(), True),
            StructField("deadlineForSubmissionOfRecommendation", StringType(), True),
            StructField("dateOfRecommendations", StringType(), True),
            StructField("stage5ExtensionToRecommendationDeadline", StringType(), True),
            StructField("deadlineForDecision", StringType(), True),
            StructField("confirmedDateOfDecision", StringType(), True),
            StructField("stage5ExtensionToDecisionDeadline", StringType(), True),
            StructField("jRPeriodEndDate", StringType(), True),
            StructField("dateProjectWithdrawn", StringType(), True),
            StructField("examTimetablePublishStatus", StringType(), True),
            StructField("twitteraccountname", StringType(), True),
            StructField("exasize", StringType(), True),
            StructField("tene", StringType(), True),
            StructField("promotername", StringType(), True),
            StructField("applicantfirstname", StringType(), True),
            StructField("applicantlastname", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("Postcode", StringType(), True),
            StructField("applicantemailaddress", StringType(), True),
            StructField("applicantwebaddress", StringType(), True),
            StructField("applicantphonenumber", StringType(), True),
            StructField("applicantdescriptionofproject", StringType(), True),
            StructField("HorizonCaseNumber", StringType(), True),
            StructField("ingested_datetime", StringType(), True),
            StructField("isMaterialChange", BooleanType(), True),
            StructField("meetings", StringType(), True),
            StructField("invoices", StringType(), True),
        ]
    )


def _service_bus_row(**overrides):
    row = {
        "NSIPProjectInfoInternalID": None,
        "caseId": 1001,
        "caseReference": "EN010001",
        "projectName": "SB Project",
        "projectNameWelsh": None,
        "projectDescription": "Service bus project description",
        "projectDescriptionWelsh": None,
        "summary": None,
        "caseCreatedDate": None,
        "decision": "approved",
        "publishStatus": "published",
        "sector": "energy",
        "projectType": "solar",
        "sourceSystem": "ODT",
        "stage": "pre-application",
        "projectLocation": "London",
        "projectLocationWelsh": None,
        "projectEmailAddress": "sb@example.com",
        "regions": ["east"],
        "transboundary": False,
        "easting": 1,
        "northing": 2,
        "welshLanguage": False,
        "mapZoomLevel": "medium",
        "secretaryOfState": "SoS",
        "datePINSFirstNotifiedOfProject": None,
        "dateProjectAppearsOnWebsite": None,
        "anticipatedSubmissionDateNonSpecific": None,
        "anticipatedDateOfSubmission": None,
        "screeningOpinionSought": None,
        "screeningOpinionIssued": None,
        "scopingOpinionSought": None,
        "scopingOpinionIssued": None,
        "section46Notification": None,
        "dateOfDCOSubmission": None,
        "deadlineForAcceptanceDecision": None,
        "dateOfDCOAcceptance": None,
        "dateOfNonAcceptance": None,
        "dateOfRepresentationPeriodOpen": None,
        "dateOfRelevantRepresentationClose": None,
        "extensionToDateRelevantRepresentationsClose": None,
        "dateRRepAppearOnWebsite": None,
        "dateIAPIDue": None,
        "rule6LetterPublishDate": None,
        "preliminaryMeetingStartDate": None,
        "notificationDateForPMAndEventsDirectlyFollowingPM": None,
        "notificationDateForEventsDeveloper": None,
        "dateSection58NoticeReceived": None,
        "confirmedStartOfExamination": None,
        "rule8LetterPublishDate": None,
        "deadlineForCloseOfExamination": None,
        "dateTimeExaminationEnds": None,
        "stage4ExtensionToExamCloseDate": None,
        "deadlineForSubmissionOfRecommendation": None,
        "dateOfRecommendations": None,
        "stage5ExtensionToRecommendationDeadline": None,
        "deadlineForDecision": None,
        "confirmedDateOfDecision": None,
        "stage5ExtensionToDecisionDeadline": None,
        "jRPeriodEndDate": None,
        "dateProjectWithdrawn": None,
        "operationsLeadId": "lead-1",
        "operationsManagerId": "mgr-1",
        "caseManagerId": "case-mgr-1",
        "nsipOfficerIds": ["officer-1"],
        "nsipAdministrationOfficerIds": ["admin-1"],
        "leadInspectorId": "lead-inspector-1",
        "inspectorIds": ["inspector-1"],
        "environmentalServicesOfficerId": "env-1",
        "legalOfficerId": "legal-1",
        "applicantId": "app-1",
        "migrationStatus": True,
        "dateOfReOpenRelevantRepresentationStart": None,
        "dateOfReOpenRelevantRepresentationClose": None,
        "IsActive": "Y",
        "examTimetablePublishStatus": None,
        "twitteraccountname": None,
        "exasize": None,
        "tene": None,
        "promotername": None,
        "applicantfirstname": None,
        "applicantlastname": None,
        "addressLine1": None,
        "addressLine2": None,
        "addressTown": None,
        "addressCounty": None,
        "Postcode": None,
        "applicantemailaddress": None,
        "applicantwebaddress": None,
        "applicantphonenumber": None,
        "applicantdescriptionofproject": None,
        "HorizonCaseNumber": None,
        "inceptionMeetingDate": None,
        "draftDocumentSubmissionDate": None,
        "programmeDocumentSubmissionDate": None,
        "estimatedScopingSubmissionDate": None,
        "consultationMilestoneAdequacyDate": None,
        "principalAreaDisagreementSummaryStmtSubmittedDate": None,
        "policyComplianceDocumentSubmittedDate": None,
        "designApproachDocumentSubmittedDate": None,
        "caAndTpEvidenceSubmittedDate": None,
        "caseTeamIssuedCommentsDate": None,
        "fastTrackAdmissionDocumentSubmittedDate": None,
        "matureOutlineControlDocumentSubmittedDate": None,
        "memLastUpdated": None,
        "multipartyApplicationCheckDocumentSubmittedDate": None,
        "programmeDocumentReviewedByEstDate": None,
        "publicSectorEqualityDutySubmittedDate": None,
        "statutoryConsultationPeriodEndDate": None,
        "submissionOfDraftDocumentsDate": None,
        "updatedProgrammeDocumentReceivedDate": None,
        "courtDecisionDate": None,
        "decisionChallengeSubmissionDate": None,
        "courtDecisionOutcomeText": None,
        "recommendation": None,
        "additionalComments": None,
        "caAndTpEvidence": None,
        "fastTrackAdmissionDocument": None,
        "meetings": None,
        "multipartyApplicationCheckDocument": None,
        "newMaturity": None,
        "numberBand2Inspectors": None,
        "numberBand3Inspectors": None,
        "programmeDocumentURI": None,
        "publicSectorEqualityDuty": None,
        "subProjectType": None,
        "tier": None,
        "s61SummaryURI": None,
        "planProcessEvidence": None,
        "issuesTracker": None,
        "essentialFastTrackComponents": None,
        "principalAreaDisagreementSummaryStmt": None,
        "policyComplianceDocument": None,
        "designApproachDocument": None,
        "matureOutlineControlDocument": None,
        "invoices": None,
        "operationsLeadIds": ["lead-1"],
        "operationsManagerIds": ["mgr-1"],
        "caseManagerIds": ["case-mgr-1"],
        "leadInspectorIds": ["lead-inspector-1"],
        "environmentalServicesOfficerIds": ["env-1"],
        "legalOfficerIds": ["legal-1"],
        "migrated": True,
        "ODTSourceSystem": "ODT",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-01-10 09:00:00",
        "ValidTo": None,
        "RowID": "",
        "isMaterialChange": False,
    }
    row.update(overrides)
    return row


def _horizon_row(**overrides):
    row = {
        "caseNodeId": 3003,
        "casereference": "EN030003",
        "projectname": "Horizon Project",
        "projectNameWelsh": None,
        "projectDescription": "Horizon project description",
        "projectDescriptionWelsh": None,
        "summary": "Horizon summary",
        "caseCreatedDate": "2024-12-01",
        "projectstatus": "Not Published",
        "sector": "transport",
        "projecttype": "rail",
        "stage": "pre-application",
        "projectLocation": "Cardiff",
        "projectLocationWelsh": "Caerdydd",
        "projectEmailAddress": "hz@example.com",
        "Region": "wales",
        "transboundary": False,
        "easting": 10,
        "northing": 20,
        "welshLanguage": True,
        "mapZoomLevel": "HIGH",
        "sos": "SoS Horizon",
        "datePINSFirstNotifiedOfProject": None,
        "dateProjectAppearsOnWebsite": None,
        "anticipatedSubmissionDateNonSpecific": None,
        "anticipatedDateOfSubmission": None,
        "screeningOpinionSought": None,
        "screeningOpinionIssued": None,
        "scopingOpinionSought": None,
        "scopingOpinionIssued": None,
        "section46Notification": None,
        "dateOfDCOSubmission": None,
        "deadlineForAcceptanceDecision": None,
        "dateOfDCOAcceptance": None,
        "dateOfRepresentationPeriodOpen": None,
        "dateOfRelevantRepresentationClose": None,
        "extensionToDateRelevantRepresentationsClose": None,
        "dateRRepAppearOnWebsite": None,
        "preliminaryMeetingStartDate": None,
        "dateSection58NoticeReceived": None,
        "confirmedStartOfExamination": None,
        "deadlineForCloseOfExamination": None,
        "dateTimeExaminationEnds": None,
        "stage4ExtensionToExamCloseDate": None,
        "deadlineForSubmissionOfRecommendation": None,
        "dateOfRecommendations": None,
        "stage5ExtensionToRecommendationDeadline": None,
        "deadlineForDecision": None,
        "confirmedDateOfDecision": None,
        "stage5ExtensionToDecisionDeadline": None,
        "jRPeriodEndDate": None,
        "dateProjectWithdrawn": None,
        "examTimetablePublishStatus": "published",
        "twitteraccountname": "@hzproject",
        "exasize": "large",
        "tene": "TEN-E",
        "promotername": "Promoter Ltd",
        "applicantfirstname": "Jane",
        "applicantlastname": "Doe",
        "addressLine1": "1 High Street",
        "addressLine2": None,
        "addressTown": "Cardiff",
        "addressCounty": "South Glamorgan",
        "Postcode": "CF10 1AA",
        "applicantemailaddress": "applicant@example.com",
        "applicantwebaddress": "https://example.com",
        "applicantphonenumber": "01234567890",
        "applicantdescriptionofproject": "Project description from applicant",
        "HorizonCaseNumber": "3003",
        "ingested_datetime": "2025-03-01 10:00:00",
        "isMaterialChange": True,
        "meetings": "[{'meetingId':'m-1','meetingAgenda':'Agenda 1','planningInspectorateRole':'Lead','meetingDate':'2025-03-01','meetingType':'Intro','estimatedPrelimMeetingDate':'2025-03-05'}]",
        "invoices": "[{'invoiceStage':'stage-1','invoiceNumber':'INV-1','amountDue':123.45,'paymentDueDate':'2025-03-31','invoicedDate':'2025-03-01','paymentDate':'2025-03-20','refundCreditNoteNumber':'CR-1','refundAmount':12.0,'refundIssueDate':'2025-03-25'}]",
    }
    row.update(overrides)
    return row


def _default_first_seen_rows(service_bus_rows):
    grouped = {}

    for row in service_bus_rows:
        case_id = row.get("caseId")
        ingestion = row.get("IngestionDate")
        if case_id is None or ingestion is None:
            continue

        if case_id not in grouped or ingestion < grouped[case_id]:
            grouped[case_id] = ingestion

    return [(case_id, ingested) for case_id, ingested in grouped.items()]


class TestNsipProjectHarmonisationProcess(SparkTestCase):
    def test__nsip_project_harmonisation_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = NsipProjectHarmonisationProcess(spark)

        assert inst.get_name() == "nsip_project_harmonisation_process"

    def test__nsip_project_harmonisation_process__process__filters_migrated_horizon_cases_and_keeps_non_migrated_history(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, caseReference="EN010001", IngestionDate="2025-01-10 09:00:00"),
        ]
        horizon_rows = [
            _horizon_row(
                caseNodeId=1001,
                HorizonCaseNumber="1001",
                casereference="EN010001",
                projectname="Migrated Horizon Copy",
                ingested_datetime="2025-02-01 10:00:00",
            ),
            _horizon_row(
                caseNodeId=3003,
                HorizonCaseNumber="3003",
                casereference="EN030003",
                projectname="Kept Horizon Record",
                ingested_datetime="2025-03-01 10:00:00",
            ),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        output_case_ids = {row["caseId"] for row in df.select("caseId").distinct().collect()}

        assert 1001 in output_case_ids
        assert 3003 in output_case_ids

        migrated_horizon_count = df.where((F.col("caseId") == 1001) & (F.col("sourceSystem") == "Horizon")).count()
        assert migrated_horizon_count == 0

    def test__nsip_project_harmonisation_process__process__filters_horizon_rows_using_first_seen_service_bus_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, IngestionDate="2025-01-10 09:00:00"),
        ]
        horizon_rows = [
            _horizon_row(
                caseNodeId=1001,
                HorizonCaseNumber="1001",
                projectname="Should Be Filtered",
                ingested_datetime="2025-02-01 10:00:00",
            ),
            _horizon_row(
                caseNodeId=3003,
                HorizonCaseNumber="3003",
                projectname="Should Remain",
                ingested_datetime="2025-03-01 10:00:00",
            ),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            [(1001, "2025-01-10 09:00:00")],
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        filtered_count = df.where((F.col("caseId") == 1001) & (F.col("sourceSystem") == "Horizon")).count()
        kept_count = df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon")).count()

        assert filtered_count == 0
        assert kept_count == 1

    def test__nsip_project_harmonisation_process__process__normalises_horizon_publish_status_and_zoom_level(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [
            _horizon_row(
                caseNodeId=3003,
                Region="wales",
                projectstatus="Not Published",
                mapZoomLevel="HIGH",
                ingested_datetime="2025-03-01 10:00:00",
            ),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon")).select("publishStatus", "mapZoomLevel").collect()[0]

        assert row["publishStatus"] == "unpublished"
        assert row["mapZoomLevel"] == "high"

    def test__nsip_project_harmonisation_process__process__aggregates_regions_using_collect_list(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [
            _horizon_row(caseNodeId=3003, Region="wales", ingested_datetime="2025-03-01 10:00:00"),
            _horizon_row(caseNodeId=3003, Region="north west", ingested_datetime="2025-03-01 10:00:00"),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon")).select("regions").collect()[0]

        assert sorted(row["regions"]) == ["north west", "wales"]

    def test__nsip_project_harmonisation_process__process__aggregates_horizon_array_like_id_fields(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [
            _horizon_row(caseNodeId=3003, Region="wales", ingested_datetime="2025-03-01 10:00:00"),
            _horizon_row(caseNodeId=3003, Region="north west", ingested_datetime="2025-03-01 10:00:00"),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = (
            df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon"))
            .select(
                "nsipOfficerIds",
                "nsipAdministrationOfficerIds",
                "inspectorIds",
                "operationsManagerIds",
                "legalOfficerIds",
                "operationsLeadIds",
                "caseManagerIds",
                "leadInspectorIds",
                "environmentalServicesOfficerIds",
            )
            .collect()[0]
        )

        assert len(row["nsipOfficerIds"]) == 2
        assert len(row["nsipAdministrationOfficerIds"]) == 2
        assert len(row["inspectorIds"]) == 2
        assert len(row["operationsManagerIds"]) == 2
        assert len(row["legalOfficerIds"]) == 2
        assert len(row["operationsLeadIds"]) == 2
        assert len(row["caseManagerIds"]) == 2
        assert len(row["leadInspectorIds"]) == 2
        assert len(row["environmentalServicesOfficerIds"]) == 2

    def test__nsip_project_harmonisation_process__process__parses_invoices_and_meetings_json_from_horizon(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [_horizon_row(caseNodeId=3003, ingested_datetime="2025-03-01 10:00:00")]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon")).select("invoices", "meetings").collect()[0]

        assert row["invoices"] is not None
        assert len(row["invoices"]) == 1
        assert row["invoices"][0]["invoiceNumber"] == "INV-1"
        assert float(row["invoices"][0]["amountDue"]) == 123.45

        assert row["meetings"] is not None
        assert len(row["meetings"]) == 1
        assert row["meetings"][0]["meetingId"] == "m-1"
        assert row["meetings"][0]["meetingType"] == "Intro"

    def test__nsip_project_harmonisation_process__process__sets_horizon_source_system_and_odt_source_system_to_horizon(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [_horizon_row(caseNodeId=3003, ingested_datetime="2025-03-01 10:00:00")]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where(F.col("caseId") == 3003).select("sourceSystem", "ODTSourceSystem").collect()[0]

        assert row["sourceSystem"] == "Horizon"
        assert row["ODTSourceSystem"] == "Horizon"

    def test__nsip_project_harmonisation_process__process__preserves_service_bus_metadata_fields(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(
                caseId=1001,
                sourceSystem="ODT",
                ODTSourceSystem="LegacyODT",
                SourceSystemID="SRC-123",
                IngestionDate="2025-01-10 09:00:00",
            )
        ]
        horizon_rows = [_horizon_row(caseNodeId=3003)]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = (
            df.where(F.col("caseId") == 1001)
            .select("sourceSystem", "ODTSourceSystem", "SourceSystemID", "IngestionDate")
            .orderBy("IngestionDate")
            .collect()[0]
        )

        assert row["sourceSystem"] == "ODT"
        assert row["ODTSourceSystem"] == "LegacyODT"
        assert row["SourceSystemID"] == "SRC-123"
        assert row["IngestionDate"] is not None

    def test__nsip_project_harmonisation_process__process__populates_horizon_default_null_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [_horizon_row(caseNodeId=3003, ingested_datetime="2025-03-01 10:00:00")]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = (
            df.where(F.col("caseId") == 3003)
            .select(
                "decision",
                "dateOfNonAcceptance",
                "dateIAPIDue",
                "rule6LetterPublishDate",
                "notificationDateForPMAndEventsDirectlyFollowingPM",
                "notificationDateForEventsDeveloper",
                "rule8LetterPublishDate",
                "migrationStatus",
                "SourceSystemID",
            )
            .collect()[0]
        )

        assert row["decision"] is None
        assert row["dateOfNonAcceptance"] is None
        assert row["dateIAPIDue"] is None
        assert row["rule6LetterPublishDate"] is None
        assert row["notificationDateForPMAndEventsDirectlyFollowingPM"] is None
        assert row["notificationDateForEventsDeveloper"] is None
        assert row["rule8LetterPublishDate"] is None
        assert row["migrationStatus"] is False
        assert row["SourceSystemID"] is None

    def test__nsip_project_harmonisation_process__process__preserves_horizon_only_fields_after_column_alignment(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [_horizon_row(caseNodeId=3003, ingested_datetime="2025-03-01 10:00:00")]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = (
            df.where(F.col("caseId") == 3003)
            .select(
                "examTimetablePublishStatus",
                "twitteraccountname",
                "exasize",
                "tene",
                "promotername",
                "applicantfirstname",
                "applicantlastname",
                "addressLine1",
                "addressTown",
                "addressCounty",
                "Postcode",
                "applicantemailaddress",
                "applicantwebaddress",
                "applicantphonenumber",
                "applicantdescriptionofproject",
                "HorizonCaseNumber",
            )
            .collect()[0]
        )

        assert row["examTimetablePublishStatus"] == "published"
        assert row["twitteraccountname"] == "@hzproject"
        assert row["exasize"] == "large"
        assert row["tene"] == "TEN-E"
        assert row["promotername"] == "Promoter Ltd"
        assert row["applicantfirstname"] == "Jane"
        assert row["applicantlastname"] == "Doe"
        assert row["addressLine1"] == "1 High Street"
        assert row["addressTown"] == "Cardiff"
        assert row["addressCounty"] == "South Glamorgan"
        assert row["Postcode"] == "CF10 1AA"
        assert row["applicantemailaddress"] == "applicant@example.com"
        assert row["applicantwebaddress"] == "https://example.com"
        assert row["applicantphonenumber"] == "01234567890"
        assert row["applicantdescriptionofproject"] == "Project description from applicant"
        assert row["HorizonCaseNumber"] == "3003"

    def test__nsip_project_harmonisation_process__process__keeps_service_bus_and_horizon_rows_via_union_by_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = [_horizon_row(caseNodeId=3003)]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        case_ids = {row["caseId"] for row in df.select("caseId").distinct().collect()}

        assert case_ids == {1001, 3003}
        assert result.metadata.insert_count == 2

    def test__nsip_project_harmonisation_process__process__drops_duplicate_rows_in_final_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, projectName="Same", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="Same", IngestionDate="2025-01-10 09:00:00"),
        ]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        case_rows = df.where(F.col("caseId") == 1001).collect()

        assert len(case_rows) == 1
        assert result.metadata.insert_count == 1

    def test__nsip_project_harmonisation_process__process__uses_overwrite_write_mode(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [_service_bus_row(caseId=1001)]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__nsip_project_harmonisation_process__process__sets_insert_count_to_final_output_row_count(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001),
            _service_bus_row(caseId=2002, caseReference="EN020002"),
        ]
        horizon_rows = [_horizon_row(caseNodeId=3003)]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert result.metadata.insert_count == df.count()

    def test__nsip_project_harmonisation_process__process__rowid_changes_when_hashed_business_fields_change(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, projectName="Version A", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="Version B", IngestionDate="2025-02-10 09:00:00"),
        ]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.where(F.col("caseId") == 1001).select("projectName", "RowID").orderBy("projectName").collect()

        assert rows[0]["RowID"]
        assert rows[1]["RowID"]
        assert rows[0]["RowID"] != rows[1]["RowID"]

    def test__nsip_project_harmonisation_process__process__sets_migrated_to_1_only_for_caseids_present_in_service_bus(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, IngestionDate="2025-01-10 09:00:00"),
        ]
        horizon_rows = [
            _horizon_row(caseNodeId=3003, HorizonCaseNumber="3003", ingested_datetime="2025-03-01 10:00:00"),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        sb_row = df.where(F.col("caseId") == 1001).select("migrated").collect()[0]
        horizon_row = df.where(F.col("caseId") == 3003).select("migrated").collect()[0]

        assert str(sb_row["migrated"]) == "1"
        assert str(horizon_row["migrated"]) == "0"

    def test__nsip_project_harmonisation_process__process__derives_valid_to_from_next_ingestion_date_per_case_only(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, projectName="V1", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="V2", IngestionDate="2025-02-10 09:00:00"),
            _service_bus_row(caseId=2002, projectName="Other Case", IngestionDate="2025-01-15 09:00:00"),
        ]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        case_1001_rows = df.where(F.col("caseId") == 1001).select("projectName", "IngestionDate", "ValidTo").orderBy("IngestionDate").collect()
        case_2002_row = df.where(F.col("caseId") == 2002).select("ValidTo").collect()[0]

        assert case_1001_rows[0]["ValidTo"] == case_1001_rows[1]["IngestionDate"]
        assert case_1001_rows[1]["ValidTo"] is None
        assert case_2002_row["ValidTo"] is None

    def test__nsip_project_harmonisation_process__process__assigns_internal_ids_globally_by_ingestiondate_then_caseid(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=2002, projectName="Later CaseId", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="Earlier CaseId", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=3003, projectName="Latest", IngestionDate="2025-02-10 09:00:00"),
        ]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.select("caseId", "IngestionDate", "NSIPProjectInfoInternalID").orderBy("NSIPProjectInfoInternalID").collect()

        assert rows[0]["caseId"] == 1001
        assert rows[1]["caseId"] == 2002
        assert rows[2]["caseId"] == 3003

    def test__nsip_project_harmonisation_process__process__marks_only_latest_row_per_case_as_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, projectName="Old", IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="New", IngestionDate="2025-02-10 09:00:00"),
        ]
        horizon_rows = []

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.where(F.col("caseId") == 1001).select("projectName", "IsActive").orderBy("projectName").collect()

        is_active_by_name = {row["projectName"]: row["IsActive"] for row in rows}

        assert is_active_by_name["Old"] == "N"
        assert is_active_by_name["New"] == "Y"

    def test__nsip_project_harmonisation_process__process__uses_min_horizon_ingested_datetime_for_grouped_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = []
        horizon_rows = [
            _horizon_row(caseNodeId=3003, Region="wales", ingested_datetime="2025-03-02 10:00:00"),
            _horizon_row(caseNodeId=3003, Region="north west", ingested_datetime="2025-03-01 09:00:00"),
        ]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("caseId", LongType(), True),
                    StructField("ingested", StringType(), True),
                ]
            ),
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where(F.col("caseId") == 3003).select("IngestionDate").collect()[0]

        assert str(row["IngestionDate"]).startswith("2025-03-01 09:00:00")

    def test__nsip_project_harmonisation_process__process__populates_rowid_for_each_output_row(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_rows = [
            _service_bus_row(caseId=1001, IngestionDate="2025-01-10 09:00:00"),
            _service_bus_row(caseId=1001, projectName="Changed Name", IngestionDate="2025-02-10 09:00:00"),
        ]
        horizon_rows = [_horizon_row(caseNodeId=3003, ingested_datetime="2025-03-01 10:00:00")]

        service_bus_data = spark.createDataFrame(service_bus_rows, schema=_service_bus_schema())
        horizon_data = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        first_seen_service_bus_data = spark.createDataFrame(
            _default_first_seen_rows(service_bus_rows),
            ["caseId", "ingested"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"):
            inst = NsipProjectHarmonisationProcess(spark)
            data_to_write, _ = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "first_seen_service_bus_data": first_seen_service_bus_data,
                }
            )

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.select("caseId", "IngestionDate", "RowID").orderBy("caseId", "IngestionDate").collect()

        assert all(row["RowID"] for row in rows)

        case_1001_rowids = [row["RowID"] for row in rows if row["caseId"] == 1001]
        assert len(case_1001_rowids) == 2
        assert case_1001_rowids[0] != case_1001_rowids[1]
