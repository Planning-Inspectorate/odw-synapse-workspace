import pytest
from odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process import NsipProjectHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import mock
from pyspark.sql import functions as F

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _service_bus_df(spark):
    return spark.createDataFrame(
        [
            (
                None,
                1001,
                "EN010001",
                "SB Project V1",
                None,
                "Service bus description v1",
                None,
                None,
                None,
                "approved",
                "published",
                "energy",
                "solar",
                "ODT",
                "pre-application",
                "London",
                None,
                "sb@example.com",
                ["east"],
                False,
                1,
                2,
                False,
                "medium",
                "SoS",
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
                None,
                None,
                "lead-1",
                "mgr-1",
                "case-mgr-1",
                ["officer-1"],
                ["admin-1"],
                "lead-inspector-1",
                ["inspector-1"],
                "env-1",
                "legal-1",
                "app-1",
                True,
                None,
                None,
                "Y",
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
                ["lead-1"],
                ["mgr-1"],
                ["case-mgr-1"],
                ["lead-inspector-1"],
                ["env-1"],
                ["legal-1"],
                True,
                "ODT",
                "SRC-1",
                "2025-01-10 09:00:00",
                None,
                "",
                False,
            ),
            (
                None,
                1001,
                "EN010001",
                "SB Project V2",
                None,
                "Service bus description v2",
                None,
                None,
                None,
                "approved",
                "published",
                "energy",
                "solar",
                "ODT",
                "pre-application",
                "London",
                None,
                "sb@example.com",
                ["east"],
                False,
                1,
                2,
                False,
                "medium",
                "SoS",
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
                None,
                None,
                "lead-1",
                "mgr-1",
                "case-mgr-1",
                ["officer-1"],
                ["admin-1"],
                "lead-inspector-1",
                ["inspector-1"],
                "env-1",
                "legal-1",
                "app-1",
                True,
                None,
                None,
                "Y",
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
                ["lead-1"],
                ["mgr-1"],
                ["case-mgr-1"],
                ["lead-inspector-1"],
                ["env-1"],
                ["legal-1"],
                True,
                "ODT",
                "SRC-1",
                "2025-02-10 09:00:00",
                None,
                "",
                False,
            ),
        ],
        [
            "NSIPProjectInfoInternalID",
            "caseId",
            "caseReference",
            "projectName",
            "projectNameWelsh",
            "projectDescription",
            "projectDescriptionWelsh",
            "summary",
            "caseCreatedDate",
            "decision",
            "publishStatus",
            "sector",
            "projectType",
            "sourceSystem",
            "stage",
            "projectLocation",
            "projectLocationWelsh",
            "projectEmailAddress",
            "regions",
            "transboundary",
            "easting",
            "northing",
            "welshLanguage",
            "mapZoomLevel",
            "secretaryOfState",
            "datePINSFirstNotifiedOfProject",
            "dateProjectAppearsOnWebsite",
            "anticipatedSubmissionDateNonSpecific",
            "anticipatedDateOfSubmission",
            "screeningOpinionSought",
            "screeningOpinionIssued",
            "scopingOpinionSought",
            "scopingOpinionIssued",
            "section46Notification",
            "dateOfDCOSubmission",
            "deadlineForAcceptanceDecision",
            "dateOfDCOAcceptance",
            "dateOfNonAcceptance",
            "dateOfRepresentationPeriodOpen",
            "dateOfRelevantRepresentationClose",
            "extensionToDateRelevantRepresentationsClose",
            "dateRRepAppearOnWebsite",
            "dateIAPIDue",
            "rule6LetterPublishDate",
            "preliminaryMeetingStartDate",
            "notificationDateForPMAndEventsDirectlyFollowingPM",
            "notificationDateForEventsDeveloper",
            "dateSection58NoticeReceived",
            "confirmedStartOfExamination",
            "rule8LetterPublishDate",
            "deadlineForCloseOfExamination",
            "dateTimeExaminationEnds",
            "stage4ExtensionToExamCloseDate",
            "deadlineForSubmissionOfRecommendation",
            "dateOfRecommendations",
            "stage5ExtensionToRecommendationDeadline",
            "deadlineForDecision",
            "confirmedDateOfDecision",
            "stage5ExtensionToDecisionDeadline",
            "jRPeriodEndDate",
            "dateProjectWithdrawn",
            "operationsLeadId",
            "operationsManagerId",
            "caseManagerId",
            "nsipOfficerIds",
            "nsipAdministrationOfficerIds",
            "leadInspectorId",
            "inspectorIds",
            "environmentalServicesOfficerId",
            "legalOfficerId",
            "applicantId",
            "migrationStatus",
            "dateOfReOpenRelevantRepresentationStart",
            "dateOfReOpenRelevantRepresentationClose",
            "IsActive",
            "examTimetablePublishStatus",
            "twitteraccountname",
            "exasize",
            "tene",
            "promotername",
            "applicantfirstname",
            "applicantlastname",
            "addressLine1",
            "addressLine2",
            "addressTown",
            "addressCounty",
            "Postcode",
            "applicantemailaddress",
            "applicantwebaddress",
            "applicantphonenumber",
            "applicantdescriptionofproject",
            "HorizonCaseNumber",
            "inceptionMeetingDate",
            "draftDocumentSubmissionDate",
            "programmeDocumentSubmissionDate",
            "estimatedScopingSubmissionDate",
            "consultationMilestoneAdequacyDate",
            "principalAreaDisagreementSummaryStmtSubmittedDate",
            "policyComplianceDocumentSubmittedDate",
            "designApproachDocumentSubmittedDate",
            "caAndTpEvidenceSubmittedDate",
            "caseTeamIssuedCommentsDate",
            "fastTrackAdmissionDocumentSubmittedDate",
            "matureOutlineControlDocumentSubmittedDate",
            "memLastUpdated",
            "multipartyApplicationCheckDocumentSubmittedDate",
            "programmeDocumentReviewedByEstDate",
            "publicSectorEqualityDutySubmittedDate",
            "statutoryConsultationPeriodEndDate",
            "submissionOfDraftDocumentsDate",
            "updatedProgrammeDocumentReceivedDate",
            "courtDecisionDate",
            "decisionChallengeSubmissionDate",
            "courtDecisionOutcomeText",
            "recommendation",
            "additionalComments",
            "caAndTpEvidence",
            "fastTrackAdmissionDocument",
            "meetings",
            "multipartyApplicationCheckDocument",
            "newMaturity",
            "numberBand2Inspectors",
            "numberBand3Inspectors",
            "programmeDocumentURI",
            "publicSectorEqualityDuty",
            "subProjectType",
            "tier",
            "s61SummaryURI",
            "planProcessEvidence",
            "issuesTracker",
            "essentialFastTrackComponents",
            "principalAreaDisagreementSummaryStmt",
            "policyComplianceDocument",
            "designApproachDocument",
            "matureOutlineControlDocument",
            "invoices",
            "operationsLeadIds",
            "operationsManagerIds",
            "caseManagerIds",
            "leadInspectorIds",
            "environmentalServicesOfficerIds",
            "legalOfficerIds",
            "migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
            "ValidTo",
            "RowID",
            "isMaterialChange",
        ],
    )


def _horizon_df(spark):
    return spark.createDataFrame(
        [
            (
                1001,
                "EN010001",
                "Migrated Horizon Version",
                None,
                "Migrated horizon description",
                None,
                "Migrated horizon summary",
                "2024-12-01",
                "Not Published",
                "transport",
                "rail",
                "pre-application",
                "Cardiff",
                "Caerdydd",
                "hz@example.com",
                "wales",
                False,
                10,
                20,
                True,
                "HIGH",
                "SoS Horizon",
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
                "published",
                "@hzproject",
                "large",
                "TEN-E",
                "Promoter Ltd",
                "Jane",
                "Doe",
                "1 High Street",
                None,
                "Cardiff",
                "South Glamorgan",
                "CF10 1AA",
                "applicant@example.com",
                "https://example.com",
                "01234567890",
                "Project description from applicant",
                "1001",
                "2025-03-01 10:00:00",
                True,
                "[{'meetingId':'m-1','meetingAgenda':'Agenda 1','planningInspectorateRole':'Lead','meetingDate':'2025-03-01','meetingType':'Intro','estimatedPrelimMeetingDate':'2025-03-05'}]",
                "[{'invoiceStage':'stage-1','invoiceNumber':'INV-1','amountDue':123.45,'paymentDueDate':'2025-03-31','invoicedDate':'2025-03-01','paymentDate':'2025-03-20','refundCreditNoteNumber':'CR-1','refundAmount':12.0,'refundIssueDate':'2025-03-25'}]",
            ),
            (
                3003,
                "EN030003",
                "Horizon Project",
                None,
                "Horizon project description",
                None,
                "Horizon summary",
                "2024-12-01",
                "Not Published",
                "transport",
                "rail",
                "pre-application",
                "Cardiff",
                "Caerdydd",
                "hz@example.com",
                "wales",
                False,
                10,
                20,
                True,
                "HIGH",
                "SoS Horizon",
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
                "published",
                "@hzproject",
                "large",
                "TEN-E",
                "Promoter Ltd",
                "Jane",
                "Doe",
                "1 High Street",
                None,
                "Cardiff",
                "South Glamorgan",
                "CF10 1AA",
                "applicant@example.com",
                "https://example.com",
                "01234567890",
                "Project description from applicant",
                "3003",
                "2025-03-01 10:00:00",
                True,
                "[{'meetingId':'m-1','meetingAgenda':'Agenda 1','planningInspectorateRole':'Lead','meetingDate':'2025-03-01','meetingType':'Intro','estimatedPrelimMeetingDate':'2025-03-05'}]",
                "[{'invoiceStage':'stage-1','invoiceNumber':'INV-1','amountDue':123.45,'paymentDueDate':'2025-03-31','invoicedDate':'2025-03-01','paymentDate':'2025-03-20','refundCreditNoteNumber':'CR-1','refundAmount':12.0,'refundIssueDate':'2025-03-25'}]",
            ),
            (
                3003,
                "EN030003",
                "Horizon Project",
                None,
                "Horizon project description",
                None,
                "Horizon summary",
                "2024-12-01",
                "Not Published",
                "transport",
                "rail",
                "pre-application",
                "Cardiff",
                "Caerdydd",
                "hz@example.com",
                "north west",
                False,
                10,
                20,
                True,
                "HIGH",
                "SoS Horizon",
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
                "published",
                "@hzproject",
                "large",
                "TEN-E",
                "Promoter Ltd",
                "Jane",
                "Doe",
                "1 High Street",
                None,
                "Cardiff",
                "South Glamorgan",
                "CF10 1AA",
                "applicant@example.com",
                "https://example.com",
                "01234567890",
                "Project description from applicant",
                "3003",
                "2025-03-01 10:00:00",
                True,
                "[{'meetingId':'m-1','meetingAgenda':'Agenda 1','planningInspectorateRole':'Lead','meetingDate':'2025-03-01','meetingType':'Intro','estimatedPrelimMeetingDate':'2025-03-05'}]",
                "[{'invoiceStage':'stage-1','invoiceNumber':'INV-1','amountDue':123.45,'paymentDueDate':'2025-03-31','invoicedDate':'2025-03-01','paymentDate':'2025-03-20','refundCreditNoteNumber':'CR-1','refundAmount':12.0,'refundIssueDate':'2025-03-25'}]",
            ),
        ],
        [
            "caseNodeId",
            "casereference",
            "projectname",
            "projectNameWelsh",
            "projectDescription",
            "projectDescriptionWelsh",
            "summary",
            "caseCreatedDate",
            "projectstatus",
            "sector",
            "projecttype",
            "stage",
            "projectLocation",
            "projectLocationWelsh",
            "projectEmailAddress",
            "Region",
            "transboundary",
            "easting",
            "northing",
            "welshLanguage",
            "mapZoomLevel",
            "sos",
            "datePINSFirstNotifiedOfProject",
            "dateProjectAppearsOnWebsite",
            "anticipatedSubmissionDateNonSpecific",
            "anticipatedDateOfSubmission",
            "screeningOpinionSought",
            "screeningOpinionIssued",
            "scopingOpinionSought",
            "scopingOpinionIssued",
            "section46Notification",
            "dateOfDCOSubmission",
            "deadlineForAcceptanceDecision",
            "dateOfDCOAcceptance",
            "dateOfRepresentationPeriodOpen",
            "dateOfRelevantRepresentationClose",
            "extensionToDateRelevantRepresentationsClose",
            "dateRRepAppearOnWebsite",
            "preliminaryMeetingStartDate",
            "dateSection58NoticeReceived",
            "confirmedStartOfExamination",
            "deadlineForCloseOfExamination",
            "dateTimeExaminationEnds",
            "stage4ExtensionToExamCloseDate",
            "deadlineForSubmissionOfRecommendation",
            "dateOfRecommendations",
            "stage5ExtensionToRecommendationDeadline",
            "deadlineForDecision",
            "confirmedDateOfDecision",
            "stage5ExtensionToDecisionDeadline",
            "jRPeriodEndDate",
            "dateProjectWithdrawn",
            "examTimetablePublishStatus",
            "twitteraccountname",
            "exasize",
            "tene",
            "promotername",
            "applicantfirstname",
            "applicantlastname",
            "addressLine1",
            "addressLine2",
            "addressTown",
            "addressCounty",
            "Postcode",
            "applicantemailaddress",
            "applicantwebaddress",
            "applicantphonenumber",
            "applicantdescriptionofproject",
            "HorizonCaseNumber",
            "ingested_datetime",
            "isMaterialChange",
            "meetings",
            "invoices",
        ],
    )


def _first_seen_service_bus_df(spark):
    return spark.createDataFrame(
        [
            (1001, "2025-01-10 09:00:00"),
        ],
        ["caseId", "ingested"],
    )


def test__nsip_project_harmonisation_process__run__end_to_end_matches_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "service_bus_data": _service_bus_df(spark),
        "horizon_data": _horizon_df(spark),
        "first_seen_service_bus_data": _first_seen_service_bus_df(spark),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"
        ) as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert df.count() == 3

    case_ids = {row["caseId"] for row in df.select("caseId").distinct().collect()}
    assert case_ids == {1001, 3003}

    migrated_horizon_count = (
        df.where((F.col("caseId") == 1001) & (F.col("sourceSystem") == "Horizon")).count()
    )
    assert migrated_horizon_count == 0

    horizon_row = (
        df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon"))
        .select(
            "projectName",
            "summary",
            "publishStatus",
            "mapZoomLevel",
            "regions",
            "migrated",
            "IsActive",
            "isMaterialChange",
        )
        .collect()[0]
    )

    assert horizon_row["projectName"] == "Horizon Project"
    assert horizon_row["summary"] == "Horizon summary"
    assert horizon_row["publishStatus"] == "unpublished"
    assert horizon_row["mapZoomLevel"] == "high"
    assert sorted(horizon_row["regions"]) == ["north west", "wales"]
    assert str(horizon_row["migrated"]) == "0"
    assert horizon_row["IsActive"] == "Y"
    assert horizon_row["isMaterialChange"] is True

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 3


def test__nsip_project_harmonisation_process__run__derives_valid_to_is_active_internal_ids_and_rowids_like_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "service_bus_data": _service_bus_df(spark),
        "horizon_data": _horizon_df(spark).where(F.col("caseNodeId") == 3003),
        "first_seen_service_bus_data": _first_seen_service_bus_df(spark),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"
        ) as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    case_1001_rows = (
        df.where(F.col("caseId") == 1001)
        .select(
            "caseId",
            "projectName",
            "IngestionDate",
            "ValidTo",
            "IsActive",
            "migrated",
            "NSIPProjectInfoInternalID",
            "RowID",
        )
        .orderBy("IngestionDate")
        .collect()
    )

    assert len(case_1001_rows) == 2

    first_row = case_1001_rows[0]
    second_row = case_1001_rows[1]

    assert first_row["projectName"] == "SB Project V1"
    assert second_row["projectName"] == "SB Project V2"
    assert first_row["IsActive"] == "N"
    assert second_row["IsActive"] == "Y"
    assert first_row["ValidTo"] == second_row["IngestionDate"]
    assert second_row["ValidTo"] is None
    assert str(first_row["migrated"]) == "1"
    assert str(second_row["migrated"]) == "1"
    assert first_row["NSIPProjectInfoInternalID"] < second_row["NSIPProjectInfoInternalID"]
    assert first_row["RowID"]
    assert second_row["RowID"]
    assert first_row["RowID"] != second_row["RowID"]

    all_ids = [
        row["NSIPProjectInfoInternalID"]
        for row in df.select("NSIPProjectInfoInternalID").orderBy("NSIPProjectInfoInternalID").collect()
    ]
    assert len(all_ids) == len(set(all_ids))
    assert min(all_ids) == 1

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 3


def test__nsip_project_harmonisation_process__run__parses_horizon_json_and_preserves_horizon_only_fields():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "service_bus_data": _service_bus_df(spark).limit(1),
        "horizon_data": _horizon_df(spark).where(F.col("caseNodeId") == 3003),
        "first_seen_service_bus_data": _first_seen_service_bus_df(spark),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"
        ) as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    row = (
        df.where((F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon"))
        .select(
            "examTimetablePublishStatus",
            "twitteraccountname",
            "promotername",
            "applicantfirstname",
            "applicantlastname",
            "addressLine1",
            "Postcode",
            "applicantemailaddress",
            "HorizonCaseNumber",
            "meetings",
            "invoices",
        )
        .collect()[0]
    )

    assert row["examTimetablePublishStatus"] == "published"
    assert row["twitteraccountname"] == "@hzproject"
    assert row["promotername"] == "Promoter Ltd"
    assert row["applicantfirstname"] == "Jane"
    assert row["applicantlastname"] == "Doe"
    assert row["addressLine1"] == "1 High Street"
    assert row["Postcode"] == "CF10 1AA"
    assert row["applicantemailaddress"] == "applicant@example.com"
    assert row["HorizonCaseNumber"] == "3003"

    assert row["meetings"] is not None
    assert len(row["meetings"]) == 1
    assert row["meetings"][0]["meetingId"] == "m-1"
    assert row["meetings"][0]["meetingAgenda"] == "Agenda 1"
    assert row["meetings"][0]["meetingType"] == "Intro"

    assert row["invoices"] is not None
    assert len(row["invoices"]) == 1
    assert row["invoices"][0]["invoiceNumber"] == "INV-1"
    assert float(row["invoices"][0]["amountDue"]) == 123.45
    assert float(row["invoices"][0]["refundAmount"]) == 12.0

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__nsip_project_harmonisation_process__run__filters_horizon_rows_using_first_seen_service_bus_data():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "service_bus_data": _service_bus_df(spark).limit(1),
        "horizon_data": _horizon_df(spark),
        "first_seen_service_bus_data": _first_seen_service_bus_df(spark),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_project_harmonisation_process.LoggingUtil"
        ) as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]

    filtered_count = df.where(
        (F.col("caseId") == 1001) & (F.col("sourceSystem") == "Horizon")
    ).count()
    kept_count = df.where(
        (F.col("caseId") == 3003) & (F.col("sourceSystem") == "Horizon")
    ).count()

    assert filtered_count == 0
    assert kept_count == 1
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2