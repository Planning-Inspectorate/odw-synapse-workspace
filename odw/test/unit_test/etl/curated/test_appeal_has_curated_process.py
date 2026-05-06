import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.core.etl.transformation.curated.appeal_has_curated_process import AppealHasCuratedProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


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
            T.StructField("reasonForNeighbourVisits", T.StringType(), True),
            T.StructField("affectedListedBuildingNumbers", T.StringType(), True),
            T.StructField("appellantCostsAppliedFor", T.StringType(), True),
            T.StructField("lpaCostsAppliedFor", T.StringType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("affectsScheduledMonument", T.StringType(), True),
            T.StructField("hasProtectedSpecies", T.StringType(), True),
            T.StructField("isAonbNationalLandscape", T.StringType(), True),
            T.StructField("hasInfrastructureLevy", T.StringType(), True),
            T.StructField("isInfrastructureLevyFormallyAdopted", T.StringType(), True),
            T.StructField("infrastructureLevyAdoptedDate", T.StringType(), True),
            T.StructField("infrastructureLevyExpectedDate", T.StringType(), True),
            T.StructField("caseworkReason", T.StringType(), True),
            T.StructField("importantInformation", T.StringType(), True),
            T.StructField("jurisdiction", T.StringType(), True),
            T.StructField("redeterminedIndicator", T.StringType(), True),
            T.StructField("dateCostsReportDespatched", T.StringType(), True),
            T.StructField("dateNotRecoveredOrDerecovered", T.StringType(), True),
            T.StructField("dateRecovered", T.StringType(), True),
            T.StructField("originalCaseDecisionDate", T.StringType(), True),
            T.StructField("targetDate", T.StringType(), True),
            T.StructField("siteGridReferenceEasting", T.StringType(), True),
            T.StructField("siteGridReferenceNorthing", T.StringType(), True),
            T.StructField("hasLandownersPermission", T.StringType(), True),
            T.StructField("isSiteInAreaOfSpecialControlAdverts", T.StringType(), True),
            T.StructField("wasApplicationRefusedDueToHighwayOrTraffic", T.StringType(), True),
            T.StructField("didAppellantSubmitCompletePhotosAndPlans", T.StringType(), True),
            T.StructField("advertDetails", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDetails", T.StringType(), True),
            T.StructField("designatedSitesNames", T.StringType(), True),
            T.StructField("lpaProcedurePreference", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDuration", T.StringType(), True),
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _minimal_harmonised_schema():
    return T.StructType(
        [
            T.StructField("caseId", T.StringType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("caseStatus", T.StringType(), True),
            T.StructField("caseType", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], _harmonised_schema())


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.curated.curated_process.CuratedProcess.__init__",
        return_value=None,
    ):
        inst = AppealHasCuratedProcess(spark)

    inst.spark = spark
    return inst


def _active_harmonised_df(spark):
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
                "2025-01-04",
                "2025-01-05",
                "Valid appeal",
                None,
                None,
                "2025-01-06",
                "2025-01-07",
                "2025-01-08",
                "Linked",
                "LEAD-001",
                "2025-01-09",
                "2025-01-10",
                "2025-01-11",
                "2025-01-12",
                "Valid",
                "2025-01-13",
                None,
                "Statement",
                None,
                None,
                None,
                "2025-01-14",
                "2025-01-15",
                "Allowed",
                "2025-01-16",
                None,
                "APP-REF-001",
                "2025-01-17",
                None,
                "2025-01-18",
                "2025-01-19",
                "Line 1",
                "Line 2",
                "Town",
                "County",
                "AA1 1AA",
                "Access",
                "Safety",
                1000.0,
                50.0,
                "Y",
                "N",
                "Y",
                "Y",
                "N",
                "Y",
                "N",
                "Y",
                "email",
                "Y",
                "Original development",
                None,
                None,
                "CASE-NEAR",
                "Neighbour address",
                "Neighbour reason",
                "LB-001",
                "N",
                "Y",
                "Full Planning",
                "N",
                "Y",
                "Y",
                "Y",
                "N",
                "2025-01-20",
                "2025-01-21",
                "Casework reason",
                "Important info",
                "HAS",
                "N",
                "2025-01-22",
                "2025-01-23",
                "2025-01-24",
                "2025-01-25",
                "2025-01-26",
                "123456",
                "654321",
                "Y",
                "N",
                "N",
                "Y",
                "Advert details",
                "Preference details",
                "Designated site",
                "Written Reps",
                "2 days",
                "SAP-001",
                "Y",
            )
        ],
        _harmonised_schema(),
    )


def _mixed_active_inactive_harmonised_df(spark):
    active = _active_harmonised_df(spark)
    inactive = spark.createDataFrame(
        [
            (
                "1002",
                "HAS-002",
                "SUB-002",
                "inactive",
                "HAS",
                "LI",
                "LPA02",
                None,
                None,
                None,
                None,
                ["Enforcement"],
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
                "N",
            )
        ],
        _harmonised_schema(),
    )
    return active.unionByName(inactive)


def _minimal_missing_columns_df(spark):
    return spark.createDataFrame(
        [
            ("1001", "HAS-001", "valid", "HAS", "Y"),
        ],
        _minimal_harmonised_schema(),
    )


def _duplicate_active_harmonised_df(spark):
    active = _active_harmonised_df(spark)
    return active.unionByName(active)


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


class TestAppealHasCuratedProcess(SparkTestCase):
    def test__appeal_has_curated_process__process__filters_active_rows_and_projects_curated_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_mixed_active_inactive_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        actual_df = df.select(
            "caseReference",
            "caseId",
            "caseStatus",
            "caseType",
            "caseProcedure",
            "lpaCode",
            "caseSpecialisms",
            "siteAreaSquareMetres",
            "floorSpaceSquareMetres",
            "importantInformation",
            "padsSapId",
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    "1001",
                    "valid",
                    "HAS",
                    "WR",
                    "LPA01",
                    ["Advertisements", "Listed Building"],
                    1000.0,
                    50.0,
                    "Important info",
                    "SAP-001",
                )
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert df.count() == 1
        assert df.columns == CURATED_COLUMNS
        assert "IsActive" not in df.columns

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_has_curated_process__process__adds_missing_curated_columns_as_null_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_minimal_missing_columns_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == CURATED_COLUMNS
        assert row["caseId"] == "1001"
        assert row["caseReference"] == "HAS-001"
        assert row["caseStatus"] == "valid"
        assert row["caseType"] == "HAS"

        for col_name in CURATED_COLUMNS:
            if col_name not in {"caseId", "caseReference", "caseStatus", "caseType"}:
                assert row[col_name] is None

        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_process__process__excludes_inactive_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inactive_df = _mixed_active_inactive_harmonised_df(spark).where("IsActive = 'N'")

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(inactive_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert result.metadata.insert_count == 0

    def test__appeal_has_curated_process__process__empty_harmonised_input_writes_empty_curated_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_empty_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert df.columns == CURATED_COLUMNS
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "parquet"
        assert result.metadata.insert_count == 0

    def test__appeal_has_curated_process__process__preserves_duplicate_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_duplicate_active_harmonised_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where("caseReference = 'HAS-001'").count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_has_curated_process__process__keeps_expected_write_config_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(_active_harmonised_df(spark)))

        write_config = data_to_write[inst.OUTPUT_TABLE]

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_process__process__only_keeps_exact_uppercase_y_active_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        active = _active_harmonised_df(spark)

        lower_y = active.withColumn("IsActive", F.lit("y"))
        spaced_y = active.withColumn("IsActive", F.lit(" Y "))
        inactive = active.withColumn("IsActive", F.lit("N"))

        source_df = lower_y.unionByName(spaced_y).unionByName(inactive)

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert result.metadata.insert_count == 0
