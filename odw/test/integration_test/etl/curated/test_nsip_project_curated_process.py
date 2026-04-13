import mock
import pytest
from odw.core.etl.transformation.curated.nsip_project_curated_process import NsipProjectCuratedProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def _harmonised_schema():
    return StructType(
        [
            StructField("caseId", IntegerType(), True),
            StructField("caseReference", StringType(), True),
            StructField("projectName", StringType(), True),
            StructField("projectNameWelsh", StringType(), True),
            StructField("projectDescription", StringType(), True),
            StructField("projectDescriptionWelsh", StringType(), True),
            StructField("decision", StringType(), True),
            StructField("publishStatus", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("projectType", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("projectLocation", StringType(), True),
            StructField("projectLocationWelsh", StringType(), True),
            StructField("projectEmailAddress", StringType(), True),
            StructField("regions", ArrayType(StringType()), True),
            StructField("transboundary", BooleanType(), True),
            StructField("easting", DoubleType(), True),
            StructField("northing", DoubleType(), True),
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
            StructField("ValidTo", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
        ]
    )


def _service_user_schema():
    return StructType(
        [
            StructField("caseReference", StringType(), True),
            StructField("serviceUserType", StringType(), True),
            StructField("id", StringType(), True),
        ]
    )


def _harmonised_row(**overrides):
    row = {
        "caseId": 1001,
        "caseReference": "EN010001",
        "projectName": "Project Latest",
        "projectNameWelsh": None,
        "projectDescription": "Latest desc",
        "projectDescriptionWelsh": None,
        "decision": "approved",
        "publishStatus": "Published",
        "sector": "energy",
        "projectType": "Normal Type",
        "ODTSourceSystem": "Horizon",
        "stage": "Pre-Application Stage",
        "projectLocation": "London",
        "projectLocationWelsh": None,
        "projectEmailAddress": "latest@example.com",
        "regions": ["east"],
        "transboundary": False,
        "easting": 11.0,
        "northing": 21.0,
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
        "caseManagerId": "case-1",
        "nsipOfficerIds": ["officer-1"],
        "nsipAdministrationOfficerIds": ["admin-1"],
        "leadInspectorId": "lead-inspector-1",
        "inspectorIds": ["inspector-1"],
        "environmentalServicesOfficerId": "env-1",
        "legalOfficerId": "legal-1",
        "applicantId": "legacy-applicant-id",
        "migrationStatus": False,
        "dateOfReOpenRelevantRepresentationStart": None,
        "dateOfReOpenRelevantRepresentationClose": None,
        "ValidTo": None,
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-02-01 10:00:00",
    }
    row.update(overrides)
    return row


def _service_user_row(**overrides):
    row = {
        "caseReference": "EN010001",
        "serviceUserType": "Applicant",
        "id": "service-user-applicant-1",
    }
    row.update(overrides)
    return row


def test__nsip_project_curated_process__run__end_to_end_matches_legacy():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "harmonised_data": spark.createDataFrame(
            [
                _harmonised_row(
                    caseId=1001,
                    caseReference="EN010001",
                    projectName="Project Old",
                    ODTSourceSystem="Horizon",
                    IngestionDate="2025-01-01 10:00:00",
                ),
                _harmonised_row(
                    caseId=1001,
                    caseReference="EN010001",
                    projectName="Project Latest",
                    projectType="WW01 - Waste Water treatment Plants",
                    ODTSourceSystem="ODT",
                    stage="Pre-Application Stage",
                    IngestionDate="2025-02-01 10:00:00",
                ),
                _harmonised_row(
                    caseId=2002,
                    caseReference="EN020002",
                    projectName="Back Office Project",
                    publishStatus="published",
                    projectType="Normal Type",
                    ODTSourceSystem="ODT",
                    stage="Post-Decision Stage",
                    projectLocation="Cardiff",
                    projectEmailAddress="bo@example.com",
                    regions=["wales"],
                    transboundary=True,
                    easting=30.0,
                    northing=40.0,
                    welshLanguage=True,
                    mapZoomLevel="high",
                    secretaryOfState="SoS 2",
                    SourceSystemID="SRC-3",
                    IngestionDate="2025-03-01 10:00:00",
                ),
                _harmonised_row(
                    caseId=3003,
                    caseReference="EN030003",
                    projectName="Non Applicant Match",
                    ODTSourceSystem="Horizon",
                    IngestionDate="2025-04-01 10:00:00",
                ),
            ],
            schema=_harmonised_schema(),
        ),
        "service_user_data": spark.createDataFrame(
            [
                _service_user_row(caseReference="EN010001", id="service-user-applicant-1"),
                _service_user_row(caseReference="EN020002", id="service-user-applicant-2"),
                _service_user_row(caseReference="EN030003", serviceUserType="Agent", id="service-user-agent-3"),
            ],
            schema=_service_user_schema(),
        ),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.curated.nsip_project_curated_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = {row["caseId"]: row.asDict(recursive=True) for row in df.collect()}

    assert df.count() == 2
    assert set(rows.keys()) == {1001, 2002}

    assert rows[1001]["projectName"] == "Project Latest"
    assert rows[1001]["publishStatus"] == "published"
    assert rows[1001]["projectType"] == "WW01 - Waste Water Treatment Plants"
    assert rows[1001]["sourceSystem"] == "back-office-applications"
    assert rows[1001]["stage"] == "pre_application_stage"
    assert rows[1001]["applicantId"] == "service-user-applicant-1"

    assert rows[2002]["sourceSystem"] == "back-office-applications"
    assert rows[2002]["stage"] == "post_decision_stage"
    assert rows[2002]["applicantId"] == "service-user-applicant-2"

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 2


def test__nsip_project_curated_process__run__keeps_only_latest_horizon_record_per_case():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "harmonised_data": spark.createDataFrame(
            [
                _harmonised_row(caseId=1001, projectName="Project Old", IngestionDate="2025-01-01 10:00:00"),
                _harmonised_row(caseId=1001, projectName="Project Latest", IngestionDate="2025-02-01 10:00:00"),
            ],
            schema=_harmonised_schema(),
        ),
        "service_user_data": spark.createDataFrame(
            [_service_user_row(caseReference="EN010001", id="service-user-applicant-1")],
            schema=_service_user_schema(),
        ),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.curated.nsip_project_curated_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = df.where(F.col("caseId") == 1001).collect()

    assert len(rows) == 1
    assert rows[0]["projectName"] == "Project Latest"
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1


def test__nsip_project_curated_process__run__filters_to_horizon_and_applicant_service_users_only():
    spark = PytestSparkSessionUtil().get_spark_session()

    source_data = {
        "harmonised_data": spark.createDataFrame(
            [
                _harmonised_row(caseId=1001, caseReference="EN010001", ODTSourceSystem="Horizon"),
                _harmonised_row(caseId=2002, caseReference="EN020002", ODTSourceSystem="LegacyODT"),
                _harmonised_row(caseId=3003, caseReference="EN030003", ODTSourceSystem="Horizon"),
            ],
            schema=_harmonised_schema(),
        ),
        "service_user_data": spark.createDataFrame(
            [
                _service_user_row(caseReference="EN010001", serviceUserType="Applicant", id="service-user-applicant-1"),
                _service_user_row(caseReference="EN020002", serviceUserType="Applicant", id="service-user-applicant-2"),
                _service_user_row(caseReference="EN030003", serviceUserType="Agent", id="service-user-agent-3"),
            ],
            schema=_service_user_schema(),
        ),
    }

    with (
        mock.patch(
            "odw.core.etl.transformation.curated.nsip_project_curated_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
        mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil") as MockProcessLogging,
    ):
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipProjectCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    df = data_to_write[inst.OUTPUT_TABLE]["data"]
    case_ids = {row["caseId"] for row in df.select("caseId").collect()}

    assert case_ids == {1001}
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
