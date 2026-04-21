import mock
from odw.core.etl.transformation.curated.nsip_project_curated_process import NsipProjectCuratedProcess
from odw.core.util.util import Util
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
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


def _harmonised_row(**overrides):
    row = {
        "caseId": 1001,
        "caseReference": "EN010001",
        "projectName": "Project Latest",
        "projectNameWelsh": "Project Latest Welsh",
        "projectDescription": "Latest desc",
        "projectDescriptionWelsh": "Latest desc Welsh",
        "decision": "approved",
        "publishStatus": "Published",
        "sector": "energy",
        "projectType": "Normal Type",
        "ODTSourceSystem": "Horizon",
        "stage": "Pre-Application Stage",
        "projectLocation": "London",
        "projectLocationWelsh": "Llundain",
        "projectEmailAddress": "latest@example.com",
        "regions": ["east"],
        "transboundary": False,
        "easting": 11.2,
        "northing": 21.8,
        "welshLanguage": False,
        "mapZoomLevel": "medium",
        "secretaryOfState": "SoS",
        "datePINSFirstNotifiedOfProject": "2025-01-01",
        "dateProjectAppearsOnWebsite": "2025-01-02",
        "anticipatedSubmissionDateNonSpecific": "later",
        "anticipatedDateOfSubmission": "2025-01-03",
        "screeningOpinionSought": "2025-01-04",
        "screeningOpinionIssued": "2025-01-05",
        "scopingOpinionSought": "2025-01-06",
        "scopingOpinionIssued": "2025-01-07",
        "section46Notification": "2025-01-08",
        "dateOfDCOSubmission": "2025-01-09",
        "deadlineForAcceptanceDecision": "2025-01-10",
        "dateOfDCOAcceptance": "2025-01-11",
        "dateOfNonAcceptance": "2025-01-12",
        "dateOfRepresentationPeriodOpen": "2025-01-13",
        "dateOfRelevantRepresentationClose": "2025-01-14",
        "extensionToDateRelevantRepresentationsClose": "2025-01-15",
        "dateRRepAppearOnWebsite": "2025-01-16",
        "dateIAPIDue": "2025-01-17",
        "rule6LetterPublishDate": "2025-01-18",
        "preliminaryMeetingStartDate": "2025-01-19",
        "notificationDateForPMAndEventsDirectlyFollowingPM": "2025-01-20",
        "notificationDateForEventsDeveloper": "2025-01-21",
        "dateSection58NoticeReceived": "2025-01-22",
        "confirmedStartOfExamination": "2025-01-23",
        "rule8LetterPublishDate": "2025-01-24",
        "deadlineForCloseOfExamination": "2025-01-25",
        "dateTimeExaminationEnds": "2025-01-26",
        "stage4ExtensionToExamCloseDate": "2025-01-27",
        "deadlineForSubmissionOfRecommendation": "2025-01-28",
        "dateOfRecommendations": "2025-01-29",
        "stage5ExtensionToRecommendationDeadline": "2025-01-30",
        "deadlineForDecision": "2025-01-31",
        "confirmedDateOfDecision": "2025-02-01",
        "stage5ExtensionToDecisionDeadline": "2025-02-02",
        "jRPeriodEndDate": "2025-02-03",
        "dateProjectWithdrawn": "2025-02-04",
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
        "migrationStatus": True,
        "dateOfReOpenRelevantRepresentationStart": "2025-02-05",
        "dateOfReOpenRelevantRepresentationClose": "2025-02-06",
        "ValidTo": None,
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-02-01 10:00:00",
    }
    row.update(overrides)
    return row


class TestNsipProjectCuratedProcess(SparkTestCase):
    def test__nsip_project_curated_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = NsipProjectCuratedProcess(spark)

        assert inst.get_name() == "nsip_project_curated_process"

    def test__nsip_project_curated_process__process__lowercases_publish_status(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [_harmonised_row(caseId=2002, caseReference="EN020002", publishStatus="PUBlic", ODTSourceSystem="Horizon")],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("publishStatus").collect()[0]

        assert row["publishStatus"] == "public"

    def test__nsip_project_curated_process__process__leaves_other_project_types_unchanged(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [_harmonised_row(caseId=2002, caseReference="EN020002", projectType="Normal Type")],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("projectType").collect()[0]

        assert row["projectType"] == "Normal Type"

    def test__nsip_project_curated_process__process__maps_odt_source_system_to_back_office_applications(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [_harmonised_row(caseId=1001, ODTSourceSystem="ODT")],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("sourceSystem").collect()[0]

        assert row["sourceSystem"] == "back-office-applications"

    def test__nsip_project_curated_process__process__lowercases_non_odt_source_system_values(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [_harmonised_row(caseId=2002, caseReference="EN020002", ODTSourceSystem="Custom-System")],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("sourceSystem").collect()[0]

        assert row["sourceSystem"] == "custom-system"

    def test__nsip_project_curated_process__process__normalises_stage_with_lowercase_spaces_and_hyphens_replaced(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [
                _harmonised_row(caseId=1001, stage="Pre-Application Stage"),
                _harmonised_row(caseId=2002, caseReference="EN020002", stage="Decision - Stage"),
            ],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = {row["caseId"]: row["stage"] for row in df.select("caseId", "stage").collect()}

        assert rows[1001] == "pre_application_stage"
        assert rows[2002] == "decision___stage"

    def test__nsip_project_curated_process__process__casts_easting_and_northing_to_int(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [_harmonised_row(easting=11.9, northing=21.1)],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        dtypes = dict(df.dtypes)

        assert dtypes["easting"] == "int"
        assert dtypes["northing"] == "int"

    def test__nsip_project_curated_process__process__uses_latest_horizon_even_if_later_non_horizon_exists(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [
                _harmonised_row(
                    caseId=6006,
                    caseReference="EN060006",
                    projectName="Latest Horizon Wins",
                    ODTSourceSystem="Horizon",
                    IngestionDate="2025-07-01 10:00:00",
                ),
                _harmonised_row(
                    caseId=6006,
                    caseReference="EN060006",
                    projectName="Later Non Horizon Should Lose",
                    ODTSourceSystem="LegacyODT",
                    IngestionDate="2025-08-01 10:00:00",
                ),
            ],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["projectName"] == "Latest Horizon Wins"

    def test__nsip_project_curated_process__process__duplicate_latest_horizon_rows_are_preserved_like_legacy_sql_join(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame(
            [
                _harmonised_row(
                    caseId=7007,
                    caseReference="EN070007",
                    projectName="Duplicate Latest A",
                    IngestionDate="2025-09-01 10:00:00",
                ),
                _harmonised_row(
                    caseId=7007,
                    caseReference="EN070007",
                    projectName="Duplicate Latest B",
                    IngestionDate="2025-09-01 10:00:00",
                ),
            ],
            schema=_harmonised_schema(),
        )

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, _ = inst.process(source_data={"harmonised_data": harmonised_df})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("caseId") == 7007).count() == 2

    def test__nsip_project_curated_process__process__uses_overwrite_write_mode_and_expected_table_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame([_harmonised_row()], schema=_harmonised_schema())

        with mock.patch("odw.core.etl.transformation.curated.nsip_project_curated_process.LoggingUtil"):
            with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
                inst = NsipProjectCuratedProcess(spark)
                data_to_write, result = inst.process(source_data={"harmonised_data": harmonised_df})

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_project"
        assert result.metadata.insert_count == data_to_write[inst.OUTPUT_TABLE]["data"].count()
