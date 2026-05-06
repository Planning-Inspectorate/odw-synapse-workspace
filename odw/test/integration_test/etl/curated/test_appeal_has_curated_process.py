import mock
import pytest
import pyspark.sql.types as T
from odw.test.util.assertion import assert_dataframes_equal
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_has_curated_process import AppealHasCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


CURATED_COLUMNS = [
    "caseId",
    "caseReference",
    "submissionId",
    "caseStatus",
    "caseType",
    "caseProcedure",
    "lpaCode",
    "caseOfficerId",
    "inspectorId",
    "allocationLevel",
    "allocationBand",
    "caseSpecialisms",
    "caseSubmittedDate",
    "caseCreatedDate",
    "caseUpdatedDate",
    "caseValidDate",
    "caseValidationDate",
    "caseValidationOutcome",
    "caseValidationInvalidDetails",
    "caseValidationIncompleteDetails",
    "caseExtensionDate",
    "caseStartedDate",
    "casePublishedDate",
    "linkedCaseStatus",
    "leadCaseReference",
    "lpaQuestionnaireDueDate",
    "lpaQuestionnaireSubmittedDate",
    "lpaQuestionnaireCreatedDate",
    "lpaQuestionnairePublishedDate",
    "lpaQuestionnaireValidationOutcome",
    "lpaQuestionnaireValidationOutcomeDate",
    "lpaQuestionnaireValidationDetails",
    "lpaStatement",
    "caseWithdrawnDate",
    "caseTransferredDate",
    "transferredCaseClosedDate",
    "caseDecisionOutcomeDate",
    "caseDecisionPublishedDate",
    "caseDecisionOutcome",
    "caseCompletedDate",
    "enforcementNotice",
    "applicationReference",
    "applicationDate",
    "applicationDecision",
    "applicationDecisionDate",
    "caseSubmissionDueDate",
    "siteAddressLine1",
    "siteAddressLine2",
    "siteAddressTown",
    "siteAddressCounty",
    "siteAddressPostcode",
    "siteAccessDetails",
    "siteSafetyDetails",
    "siteAreaSquareMetres",
    "floorSpaceSquareMetres",
    "isCorrectAppealType",
    "isGreenBelt",
    "inConservationArea",
    "ownsAllLand",
    "ownsSomeLand",
    "knowsOtherOwners",
    "knowsAllOwners",
    "advertisedAppeal",
    "notificationMethod",
    "ownersInformed",
    "originalDevelopmentDescription",
    "changedDevelopmentDescription",
    "newConditionDetails",
    "nearbyCaseReferences",
    "neighbouringSiteAddresses",
    "reasonForNeighbourVisits",
    "affectedListedBuildingNumbers",
    "appellantCostsAppliedFor",
    "lpaCostsAppliedFor",
    "typeOfPlanningApplication",
    "affectsScheduledMonument",
    "hasProtectedSpecies",
    "isAonbNationalLandscape",
    "hasInfrastructureLevy",
    "isInfrastructureLevyFormallyAdopted",
    "infrastructureLevyAdoptedDate",
    "infrastructureLevyExpectedDate",
    "caseworkReason",
    "importantInformation",
    "jurisdiction",
    "redeterminedIndicator",
    "dateCostsReportDespatched",
    "dateNotRecoveredOrDerecovered",
    "dateRecovered",
    "originalCaseDecisionDate",
    "targetDate",
    "siteGridReferenceEasting",
    "siteGridReferenceNorthing",
    "hasLandownersPermission",
    "isSiteInAreaOfSpecialControlAdverts",
    "wasApplicationRefusedDueToHighwayOrTraffic",
    "didAppellantSubmitCompletePhotosAndPlans",
    "advertDetails",
    "lpaProcedurePreferenceDetails",
    "designatedSitesNames",
    "lpaProcedurePreference",
    "lpaProcedurePreferenceDuration",
    "padsSapId",
]


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("caseId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
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
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "1001",
                "HAS-001",
                "SUB-001",
                "valid",
                "HAS",
                "WR",
                "LPA01",
                "OFFICER-001",
                "INSPECTOR-001",
                "Level 1",
                "Band A",
                ["Advertisements", "Listed Building"],
                "2025-01-01",
                "2025-01-02",
                "2025-01-03",
                "Valid appeal",
                "2025-01-04",
                "Linked",
                "LEAD-001",
                "2025-01-05",
                "2025-01-06",
                None,
                "2025-01-07",
                "Allowed",
                "2025-01-08",
                "APP-REF-001",
                "2025-01-09",
                "2025-01-10",
                "Line 1",
                "Line 2",
                "Town 1",
                "County 1",
                "AA1 1AA",
                1000.0,
                50.0,
                "Y",
                "N",
                "Development 1",
                "Full Planning",
                "2025-01-11",
                "2025-01-12",
                "2025-01-13",
                "Important info 1",
                "Y",
                "HAS",
                "Written Reps",
                "2025-01-14",
                "N",
                "123",
                "456",
                "SAP-001",
                "Y",
            ),
            (
                "1002",
                "HAS-002",
                "SUB-002",
                "inactive",
                "HAS",
                "LI",
                "LPA02",
                "OFFICER-002",
                "INSPECTOR-002",
                "Level 2",
                "Band B",
                ["Enforcement"],
                "2025-02-01",
                "2025-02-02",
                "2025-02-03",
                "Invalid appeal",
                "2025-02-04",
                "Not linked",
                "LEAD-002",
                "2025-02-05",
                "2025-02-06",
                None,
                "2025-02-07",
                "Dismissed",
                "2025-02-08",
                "APP-REF-002",
                "2025-02-09",
                "2025-02-10",
                "Line 3",
                "Line 4",
                "Town 2",
                "County 2",
                "BB1 1BB",
                2000.0,
                75.0,
                "N",
                "Y",
                "Development 2",
                "Householder",
                "2025-02-11",
                "2025-02-12",
                "2025-02-13",
                "Important info 2",
                "N",
                "HAS",
                "Inquiry",
                "2025-02-14",
                "Y",
                "789",
                "012",
                "SAP-002",
                "N",
            ),
        ],
        _harmonised_schema(),
    )


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


class TestAppealHasCuratedProcess(ETLTestCase):
    def test__appeal_has_curated_process__run__curates_active_harmonised_rows_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_harmonised_df(spark))

        inst = AppealHasCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 1
        assert df.columns == CURATED_COLUMNS
        assert "IsActive" not in df.columns

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 1,
            "update_count": 0,
            "delete_count": 0,
        }

        actual_df = df.select(
            "caseId",
            "caseReference",
            "submissionId",
            "caseStatus",
            "caseType",
            "caseProcedure",
            "lpaCode",
            "caseOfficerId",
            "inspectorId",
            "allocationLevel",
            "allocationBand",
            "caseSpecialisms",
            "caseCreatedDate",
            "caseUpdatedDate",
            "caseValidationDate",
            "caseValidationOutcome",
            "linkedCaseStatus",
            "leadCaseReference",
            "lpaQuestionnaireDueDate",
            "lpaQuestionnaireSubmittedDate",
            "caseDecisionOutcomeDate",
            "caseDecisionOutcome",
            "caseCompletedDate",
            "applicationReference",
            "applicationDate",
            "applicationDecisionDate",
            "siteAddressLine1",
            "siteAddressLine2",
            "siteAddressTown",
            "siteAddressCounty",
            "siteAddressPostcode",
            "siteAreaSquareMetres",
            "floorSpaceSquareMetres",
            "isGreenBelt",
            "inConservationArea",
            "originalDevelopmentDescription",
            "typeOfPlanningApplication",
            "dateCostsReportDespatched",
            "dateNotRecoveredOrDerecovered",
            "dateRecovered",
            "importantInformation",
            "isAonbNationalLandscape",
            "jurisdiction",
            "lpaProcedurePreference",
            "originalCaseDecisionDate",
            "redeterminedIndicator",
            "siteGridReferenceEasting",
            "siteGridReferenceNorthing",
            "padsSapId",
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "1001",
                    "HAS-001",
                    "SUB-001",
                    "valid",
                    "HAS",
                    "WR",
                    "LPA01",
                    "OFFICER-001",
                    "INSPECTOR-001",
                    "Level 1",
                    "Band A",
                    ["Advertisements", "Listed Building"],
                    "2025-01-02",
                    "2025-01-03",
                    "2025-01-03",
                    "Valid appeal",
                    "Linked",
                    "LEAD-001",
                    "2025-01-05",
                    "2025-01-06",
                    "2025-01-07",
                    "Allowed",
                    "2025-01-08",
                    "APP-REF-001",
                    "2025-01-09",
                    "2025-01-10",
                    "Line 1",
                    "Line 2",
                    "Town 1",
                    "County 1",
                    "AA1 1AA",
                    1000.0,
                    50.0,
                    "Y",
                    "N",
                    "Development 1",
                    "Full Planning",
                    "2025-01-11",
                    "2025-01-12",
                    "2025-01-13",
                    "Important info 1",
                    "Y",
                    "HAS",
                    "Written Reps",
                    "2025-01-14",
                    "N",
                    "123",
                    "456",
                    "SAP-001",
                )
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

    def test__appeal_has_curated_process__run__empty_harmonised_source_writes_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(_empty_harmonised_df(spark))

        inst = AppealHasCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 0,
            "update_count": 0,
            "delete_count": 0,
        }

    def test__appeal_has_curated_process__run__adds_missing_columns_as_null_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        minimal_df = spark.createDataFrame(
            [("1001", "HAS-001", "valid", "HAS", "Y")],
            T.StructType(
                [
                    T.StructField("caseId", T.StringType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("caseStatus", T.StringType(), True),
                    T.StructField("caseType", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        source_data = _source_data(minimal_df)

        inst = AppealHasCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        df = mock_write.call_args[0][0][inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == CURATED_COLUMNS

        for col_name in CURATED_COLUMNS:
            if col_name not in {"caseId", "caseReference", "caseStatus", "caseType"}:
                assert row[col_name] is None
        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {
            "insert_count": 1,
            "update_count": 0,
            "delete_count": 0,
        }
