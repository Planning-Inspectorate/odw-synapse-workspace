import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_has_curated_process import AppealHasCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


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


def _harmonised_df(spark: SparkSession):
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


def _curated_schema():
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
            T.StructField("caseSpecialisms", T.ArrayType(T.StringType(), True), True),
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


def _curated_row(**overrides):
    base = {
        "caseId": "1001",
        "caseReference": "HAS-001",
        "submissionId": "SUB-001",
        "caseStatus": "valid",
        "caseType": "HAS",
        "caseProcedure": "WR",
        "lpaCode": "LPA01",
        "caseOfficerId": "OFFICER-001",
        "inspectorId": "INSPECTOR-001",
        "allocationLevel": "Level 1",
        "allocationBand": "Band A",
        "caseSpecialisms": ["Advertisements", "Listed Building"],
        "caseCreatedDate": "2025-01-01",
        "caseUpdatedDate": "2025-01-02",
        "caseValidationDate": "2025-01-03",
        "caseValidationOutcome": "Valid appeal",
        "caseStartedDate": "2025-01-04",
        "linkedCaseStatus": "Linked",
        "leadCaseReference": "LEAD-001",
        "lpaQuestionnaireDueDate": "2025-01-05",
        "lpaQuestionnaireSubmittedDate": "2025-01-06",
        "caseWithdrawnDate": None,
        "caseDecisionOutcomeDate": "2025-01-07",
        "caseDecisionOutcome": "Allowed",
        "caseCompletedDate": "2025-01-08",
        "applicationReference": "APP-REF-001",
        "applicationDate": "2025-01-09",
        "applicationDecisionDate": "2025-01-10",
        "siteAddressLine1": "Line 1",
        "siteAddressLine2": "Line 2",
        "siteAddressTown": "Town 1",
        "siteAddressCounty": "County 1",
        "siteAddressPostcode": "AA1 1AA",
        "siteAreaSquareMetres": 1000.0,
        "floorSpaceSquareMetres": 50.0,
        "isGreenBelt": "Y",
        "inConservationArea": "N",
        "originalDevelopmentDescription": "Development 1",
        "typeOfPlanningApplication": "Full Planning",
        "dateCostsReportDespatched": "2025-01-11",
        "dateNotRecoveredOrDerecovered": "2025-01-12",
        "dateRecovered": "2025-01-13",
        "importantInformation": "Important info 1",
        "isAonbNationalLandscape": "Y",
        "jurisdiction": "HAS",
        "lpaProcedurePreference": "Written Reps",
        "originalCaseDecisionDate": "2025-01-14",
        "redeterminedIndicator": "N",
        "siteGridReferenceEasting": "123",
        "siteGridReferenceNorthing": "456",
        "padsSapId": "SAP-001",
        "IsActive": "Y",
    }
    return base | overrides


class TestAppealHasCuratedProcess(ETLTestCase):
    def assert_curated_etl(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()
        appeal_has = _harmonised_df(spark)
        appeal_has_table = f"{test_case}_appeal_has"
        self.write_existing_table(spark, appeal_has, appeal_has_table, "odw_harmonised_db", "odw-harmonised", appeal_has_table, "overwrite")
        expected_curated_data_after_writing = spark.createDataFrame((_curated_row(),), schema=_curated_schema())
        with mock.patch.object(AppealHasCuratedProcess, "OUTPUT_TABLE", appeal_has_table):
            inst = AppealHasCuratedProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)
            actual_table_data = spark.table(appeal_has_table)
            assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__appeal_has_curated_process__run__with_no_existing_data(self):
        """
        - Given I have harmonised appeal_has data
        - When I call AppealHasCuratedMipinsProcess.run
        - Then the data should be transformed and the appeals_has_curated_mipins table should be created
        """
        self.assert_curated_etl("t_ahcp_r_wned")

    def test__appeal_has_curated_process__run__with_existing_data(self):
        """
        - Given I have harmonised appeal_has data and an existing curated appeal_has table
        - When I call AppealHasCuratedMipinsProcess.run
        - Then the data should be transformed and the appeals_has_curated_mipins table should be created
        """
        test_case = "t_ahcp_r_wed"
        output_table = f"{test_case}_appeals_has"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_data = spark.createDataFrame((_curated_row(caseId="1"),), schema=_curated_schema())
        self.assert_curated_etl(test_case)
        self.write_existing_table(spark, existing_data, output_table, "odw_curated_db", "odw-curated", output_table, "overwrite")
        self.assert_curated_etl(test_case)
