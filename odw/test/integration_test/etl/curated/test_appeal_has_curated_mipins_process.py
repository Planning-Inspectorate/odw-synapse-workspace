from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from odw.test.util.assertion import assert_etl_result_successful, assert_dataframes_equal
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_has_curated_mipins_process import AppealHasCuratedMipinsProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from decimal import Decimal

pytestmark = pytest.mark.xfail(reason="Curated MIPINS logic not implemented yet")


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
            T.StructField("caseSubmittedDate", T.TimestampType(), True),
            T.StructField("caseCreatedDate", T.TimestampType(), True),
            T.StructField("caseUpdatedDate", T.TimestampType(), True),
            T.StructField("caseValidDate", T.TimestampType(), True),
            T.StructField("caseValidationDate", T.TimestampType(), True),
            T.StructField("caseValidationOutcome", T.StringType(), True),
            T.StructField("caseValidationInvalidDetails", T.StringType(), True),
            T.StructField("caseValidationIncompleteDetails", T.ArrayType(T.StringType()), True),
            T.StructField("caseExtensionDate", T.TimestampType(), True),
            T.StructField("caseStartedDate", T.TimestampType(), True),
            T.StructField("casePublishedDate", T.TimestampType(), True),
            T.StructField("linkedCaseStatus", T.StringType(), True),
            T.StructField("leadCaseReference", T.StringType(), True),
            T.StructField("lpaQuestionnaireDueDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireSubmittedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireCreatedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnairePublishedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireValidationOutcome", T.StringType(), True),
            T.StructField("lpaQuestionnaireValidationOutcomeDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireValidationDetails", T.ArrayType(T.StringType()), True),
            T.StructField("caseWithdrawnDate", T.TimestampType(), True),
            T.StructField("caseTransferredDate", T.TimestampType(), True),
            T.StructField("transferredCaseClosedDate", T.TimestampType(), True),
            T.StructField("caseDecisionOutcomeDate", T.TimestampType(), True),
            T.StructField("caseDecisionPublishedDate", T.TimestampType(), True),
            T.StructField("caseDecisionOutcome", T.StringType(), True),
            T.StructField("caseCompletedDate", T.TimestampType(), True),
            T.StructField("enforcementNotice", T.StringType(), True),
            T.StructField("applicationReference", T.StringType(), True),
            T.StructField("applicationDate", T.TimestampType(), True),
            T.StructField("applicationDecision", T.StringType(), True),
            T.StructField("applicationDecisionDate", T.TimestampType(), True),
            T.StructField("caseSubmissionDueDate", T.TimestampType(), True),
            T.StructField("siteAddressLine1", T.StringType(), True),
            T.StructField("siteAddressLine2", T.StringType(), True),
            T.StructField("siteAddressTown", T.StringType(), True),
            T.StructField("siteAddressCounty", T.StringType(), True),
            T.StructField("siteAddressPostcode", T.StringType(), True),
            T.StructField("siteAccessDetails", T.ArrayType(T.StringType()), True),
            T.StructField("siteSafetyDetails", T.StringType(), True),
            T.StructField("siteAreaSquareMetres", T.StringType(), True),
            T.StructField("floorSpaceSquareMetres", T.StringType(), True),
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
            T.StructField("neighbouringSiteAddresses", T.ArrayType(T.StringType()), True),
            T.StructField("affectedListedBuildingNumbers", T.StringType(), True),
            T.StructField("appellantCostsAppliedFor", T.StringType(), True),
            T.StructField("lpaCostsAppliedFor", T.StringType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("hasLandownersPermission", T.StringType(), True),
            T.StructField("wasApplicationRefusedDueToHighwayOrTraffic", T.StringType(), True),
            T.StructField("didAppellantSubmitCompletePhotosAndPlans", T.StringType(), True),
            T.StructField("isSiteInAreaOfSpecialControlAdverts", T.StringType(), True),
            T.StructField("advertDetails", T.StringType(), True),
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("applicationDecisionDueDate", T.TimestampType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("IsActive", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
        ]
    )


def _base_row(**overrides):
    row = {field.name: None for field in _harmonised_schema()}
    row.update(
        {
            "caseId": "7000001",
            "caseReference": "7000001",
            "submissionId": "SUB-001",
            "caseStatus": "valid",
            "caseType": "HAS",
            "caseProcedure": "WR",
            "lpaCode": "Q1234",
            "caseOfficerId": "OFFICER-001",
            "inspectorId": "INSPECTOR-001",
            "allocationLevel": "Level 1",
            "allocationBand": "7",
            "caseSpecialisms": ["Advertisements"],
            "caseSubmittedDate": datetime(2025, 6, 1, 12, 0),
            "caseCreatedDate": datetime(2025, 6, 1, 12, 0),
            "caseUpdatedDate": datetime(2025, 6, 1, 12, 0),
            "caseValidDate": datetime(2025, 6, 1, 12, 0),
            "caseValidationDate": datetime(2025, 6, 1, 12, 0),
            "caseValidationOutcome": "Valid",
            "caseValidationInvalidDetails": "Invalid details",
            "caseValidationIncompleteDetails": ["Missing plan"],
            "caseExtensionDate": datetime(2025, 6, 1, 12, 0),
            "caseStartedDate": datetime(2025, 6, 1, 12, 0),
            "casePublishedDate": datetime(2025, 6, 1, 12, 0),
            "linkedCaseStatus": "Linked",
            "leadCaseReference": "LEAD-001",
            "lpaQuestionnaireDueDate": datetime(2025, 6, 1, 12, 0),
            "lpaQuestionnaireSubmittedDate": datetime(2025, 6, 1, 12, 0),
            "lpaQuestionnaireCreatedDate": datetime(2025, 6, 1, 12, 0),
            "lpaQuestionnairePublishedDate": datetime(2025, 6, 1, 12, 0),
            "lpaQuestionnaireValidationOutcome": "Valid",
            "lpaQuestionnaireValidationOutcomeDate": datetime(2025, 6, 1, 12, 0),
            "lpaQuestionnaireValidationDetails": ["Accepted"],
            "caseDecisionOutcomeDate": datetime(2025, 6, 1, 12, 0),
            "caseDecisionPublishedDate": datetime(2025, 6, 1, 12, 0),
            "caseDecisionOutcome": "Allowed",
            "caseCompletedDate": datetime(2025, 6, 1, 12, 0),
            "enforcementNotice": "true",
            "applicationReference": "APP-001",
            "applicationDate": datetime(2025, 6, 1, 12, 0),
            "applicationDecision": "Granted",
            "applicationDecisionDate": datetime(2025, 6, 1, 12, 0),
            "caseSubmissionDueDate": datetime(2025, 6, 1, 12, 0),
            "siteAddressLine1": "Line 1",
            "siteAddressLine2": "Line 2",
            "siteAddressTown": "Town",
            "siteAddressCounty": "County",
            "siteAddressPostcode": "AA1 1AA",
            "siteAccessDetails": ["Gate code"],
            "siteSafetyDetails": "Safety details",
            "siteAreaSquareMetres": "true",
            "floorSpaceSquareMetres": "123.45",
            "isCorrectAppealType": "true",
            "isGreenBelt": "false",
            "inConservationArea": "true",
            "ownsAllLand": "false",
            "ownsSomeLand": "true",
            "knowsOtherOwners": "Yes",
            "knowsAllOwners": "No",
            "advertisedAppeal": "true",
            "notificationMethod": "email",
            "ownersInformed": "false",
            "originalDevelopmentDescription": "Original development",
            "changedDevelopmentDescription": "true",
            "newConditionDetails": "Conditions",
            "nearbyCaseReferences": "7000002",
            "neighbouringSiteAddresses": ["Neighbour address"],
            "affectedListedBuildingNumbers": "LB-001",
            "appellantCostsAppliedFor": "true",
            "lpaCostsAppliedFor": "false",
            "typeOfPlanningApplication": "Full Planning",
            "hasLandownersPermission": "Y",
            "wasApplicationRefusedDueToHighwayOrTraffic": "N",
            "didAppellantSubmitCompletePhotosAndPlans": "Y",
            "isSiteInAreaOfSpecialControlAdverts": "N",
            "advertDetails": "Advert details",
            "padsSapId": "SAP-001",
            "applicationDecisionDueDate": datetime(2025, 6, 1, 12, 0),
            "IngestionDate": datetime(2025, 6, 1, 12, 0),
            "ValidTo": datetime(2025, 6, 1, 12, 0),
            "IsActive": "Y",
            "ODTSourceSystem": "ODT",
        }
    )
    row.update(overrides)
    return row


def _curated_schema():
    return T.StructType(
        [
            T.StructField("caseId", T.IntegerType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("submissionId", T.StringType(), True),
            T.StructField("caseStatus", T.StringType(), True),
            T.StructField("caseType", T.StringType(), True),
            T.StructField("caseProcedure", T.StringType(), True),
            T.StructField("lpaCode", T.StringType(), True),
            T.StructField("caseOfficerId", T.StringType(), True),
            T.StructField("inspectorId", T.StringType(), True),
            T.StructField("allocationLevel", T.StringType(), True),
            T.StructField("allocationBand", T.DecimalType(10, 0), True),
            T.StructField("caseSpecialisms", T.ArrayType(T.StringType(), True), True),
            T.StructField("caseSubmittedDate", T.TimestampType(), True),
            T.StructField("caseCreatedDate", T.TimestampType(), True),
            T.StructField("caseUpdatedDate", T.TimestampType(), True),
            T.StructField("caseValidDate", T.TimestampType(), True),
            T.StructField("caseValidationDate", T.TimestampType(), True),
            T.StructField("caseValidationOutcome", T.StringType(), True),
            T.StructField("caseValidationInvalidDetails", T.StringType(), True),
            T.StructField("caseValidationIncompleteDetails", T.StringType(), True),
            T.StructField("caseExtensionDate", T.TimestampType(), True),
            T.StructField("caseStartedDate", T.TimestampType(), True),
            T.StructField("casePublishedDate", T.TimestampType(), True),
            T.StructField("linkedCaseStatus", T.StringType(), True),
            T.StructField("leadCaseReference", T.StringType(), True),
            T.StructField("lpaQuestionnaireDueDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireSubmittedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireCreatedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnairePublishedDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireValidationOutcome", T.StringType(), True),
            T.StructField("lpaQuestionnaireValidationOutcomeDate", T.TimestampType(), True),
            T.StructField("lpaQuestionnaireValidationDetails", T.StringType(), True),
            T.StructField("caseWithdrawnDate", T.TimestampType(), True),
            T.StructField("caseTransferredDate", T.TimestampType(), True),
            T.StructField("transferredCaseClosedDate", T.TimestampType(), True),
            T.StructField("caseDecisionOutcomeDate", T.TimestampType(), True),
            T.StructField("caseDecisionPublishedDate", T.TimestampType(), True),
            T.StructField("caseDecisionOutcome", T.StringType(), True),
            T.StructField("caseCompletedDate", T.TimestampType(), True),
            T.StructField("enforcementNotice", T.BooleanType(), True),
            T.StructField("applicationReference", T.StringType(), True),
            T.StructField("applicationDate", T.TimestampType(), True),
            T.StructField("applicationDecision", T.StringType(), True),
            T.StructField("applicationDecisionDate", T.TimestampType(), True),
            T.StructField("caseSubmissionDueDate", T.TimestampType(), True),
            T.StructField("siteAddressLine1", T.StringType(), True),
            T.StructField("siteAddressLine2", T.StringType(), True),
            T.StructField("siteAddressTown", T.StringType(), True),
            T.StructField("siteAddressCounty", T.StringType(), True),
            T.StructField("siteAddressPostcode", T.StringType(), True),
            T.StructField("siteAccessDetails", T.StringType(), True),
            T.StructField("siteSafetyDetails", T.StringType(), True),
            T.StructField("siteAreaSquareMetres", T.BooleanType(), True),
            T.StructField("floorSpaceSquareMetres", T.DecimalType(10, 0), True),
            T.StructField("isCorrectAppealType", T.BooleanType(), True),
            T.StructField("isGreenBelt", T.BooleanType(), True),
            T.StructField("inConservationArea", T.BooleanType(), True),
            T.StructField("ownsAllLand", T.BooleanType(), True),
            T.StructField("ownsSomeLand", T.BooleanType(), True),
            T.StructField("knowsOtherOwners", T.StringType(), True),
            T.StructField("knowsAllOwners", T.StringType(), True),
            T.StructField("advertisedAppeal", T.BooleanType(), True),
            T.StructField("notificationMethod", T.StringType(), True),
            T.StructField("ownersInformed", T.BooleanType(), True),
            T.StructField("originalDevelopmentDescription", T.StringType(), True),
            T.StructField("changedDevelopmentDescription", T.BooleanType(), True),
            T.StructField("newConditionDetails", T.StringType(), True),
            T.StructField("nearbyCaseReferences", T.StringType(), True),
            T.StructField("neighbouringSiteAddresses", T.StringType(), True),
            T.StructField("affectedListedBuildingNumbers", T.StringType(), True),
            T.StructField("appellantCostsAppliedFor", T.BooleanType(), True),
            T.StructField("lpaCostsAppliedFor", T.BooleanType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("hasLandownersPermission", T.StringType(), True),
            T.StructField("wasApplicationRefusedDueToHighwayOrTraffic", T.StringType(), True),
            T.StructField("didAppellantSubmitCompletePhotosAndPlans", T.StringType(), True),
            T.StructField("isSiteInAreaOfSpecialControlAdverts", T.StringType(), True),
            T.StructField("advertDetails", T.StringType(), True),
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("applicationDecisionDueDate", T.TimestampType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _curated_row(**overrides):
    base = {
        "caseId": 7000001,
        "caseReference": "7000001",
        "submissionId": "SUB-001",
        "caseStatus": "valid",
        "caseType": "HAS",
        "caseProcedure": "WR",
        "lpaCode": "Q1234",
        "caseOfficerId": "OFFICER-001",
        "inspectorId": "INSPECTOR-001",
        "allocationLevel": "Level 1",
        "allocationBand": Decimal("7"),
        "caseSpecialisms": ["Advertisements"],
        "caseSubmittedDate": datetime(2025, 6, 1, 13, 0),
        "caseCreatedDate": datetime(2025, 6, 1, 13, 0),
        "caseUpdatedDate": datetime(2025, 6, 1, 13, 0),
        "caseValidDate": datetime(2025, 6, 1, 13, 0),
        "caseValidationDate": datetime(2025, 6, 1, 13, 0),
        "caseValidationOutcome": "Valid",
        "caseValidationInvalidDetails": "Invalid details",
        "caseValidationIncompleteDetails": "[Missing plan]",
        "caseExtensionDate": datetime(2025, 6, 1, 13, 0),
        "caseStartedDate": datetime(2025, 6, 1, 13, 0),
        "casePublishedDate": datetime(2025, 6, 1, 13, 0),
        "linkedCaseStatus": "Linked",
        "leadCaseReference": "LEAD-001",
        "lpaQuestionnaireDueDate": datetime(2025, 6, 1, 13, 0),
        "lpaQuestionnaireSubmittedDate": datetime(2025, 6, 1, 13, 0),
        "lpaQuestionnaireCreatedDate": datetime(2025, 6, 1, 13, 0),
        "lpaQuestionnairePublishedDate": datetime(2025, 6, 1, 13, 0),
        "lpaQuestionnaireValidationOutcome": "Valid",
        "lpaQuestionnaireValidationOutcomeDate": datetime(2025, 6, 1, 13, 0),
        "lpaQuestionnaireValidationDetails": "[Accepted]",
        "caseWithdrawnDate": None,
        "caseTransferredDate": None,
        "transferredCaseClosedDate": None,
        "caseDecisionOutcomeDate": datetime(2025, 6, 1, 13, 0),
        "caseDecisionPublishedDate": datetime(2025, 6, 1, 13, 0),
        "caseDecisionOutcome": "Allowed",
        "caseCompletedDate": datetime(2025, 6, 1, 13, 0),
        "enforcementNotice": True,
        "applicationReference": "APP-001",
        "applicationDate": datetime(2025, 6, 1, 12, 0),
        "applicationDecision": "Granted",
        "applicationDecisionDate": datetime(2025, 6, 1, 13, 0),
        "caseSubmissionDueDate": datetime(2025, 6, 1, 13, 0),
        "siteAddressLine1": "Line 1",
        "siteAddressLine2": "Line 2",
        "siteAddressTown": "Town",
        "siteAddressCounty": "County",
        "siteAddressPostcode": "AA1 1AA",
        "siteAccessDetails": "[Gate code]",
        "siteSafetyDetails": "Safety details",
        "siteAreaSquareMetres": True,
        "floorSpaceSquareMetres": Decimal("123"),
        "isCorrectAppealType": True,
        "isGreenBelt": False,
        "inConservationArea": True,
        "ownsAllLand": False,
        "ownsSomeLand": True,
        "knowsOtherOwners": "Yes",
        "knowsAllOwners": "No",
        "advertisedAppeal": True,
        "notificationMethod": "email",
        "ownersInformed": False,
        "originalDevelopmentDescription": "Original development",
        "changedDevelopmentDescription": True,
        "newConditionDetails": "Conditions",
        "nearbyCaseReferences": "7000002",
        "neighbouringSiteAddresses": "[Neighbour address]",
        "affectedListedBuildingNumbers": "LB-001",
        "appellantCostsAppliedFor": True,
        "lpaCostsAppliedFor": False,
        "typeOfPlanningApplication": "Full Planning",
        "hasLandownersPermission": "Y",
        "wasApplicationRefusedDueToHighwayOrTraffic": "N",
        "didAppellantSubmitCompletePhotosAndPlans": "Y",
        "isSiteInAreaOfSpecialControlAdverts": "N",
        "advertDetails": "Advert details",
        "padsSapId": "SAP-001",
        "applicationDecisionDueDate": datetime(2025, 6, 1, 13, 0),
        "IngestionDate": datetime(2025, 6, 1, 13, 0),
        "ValidTo": datetime(2025, 6, 1, 13, 0),
        "IsActive": "Y",
    }
    return base | overrides


class TestAppealHasCuratedMipinsProcess(ETLTestCase):
    """
    Integration style run() tests for the MIPINS curated HAS process
    These tests patch load_data/write_data so they can exercise the full ETL run()
    flow without writing to ADLS while still validating the legacy MIPINS filters,
    casts, output columns and write configuration
    """

    def assert_curated_etl(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()
        appeal_has = spark.createDataFrame((_base_row(), _base_row()), schema=_harmonised_schema())
        appeal_has_table = f"{test_case}_appeal_has"
        self.write_existing_table(spark, appeal_has, appeal_has_table, "odw_harmonised_db", "odw-harmonised", appeal_has_table, "overwrite")
        expected_curated_data_after_writing = spark.createDataFrame((_curated_row(),), schema=_curated_schema())
        output_table = f"{test_case}_appeals_has_curated_mipins"
        with mock.patch.object(AppealHasCuratedMipinsProcess, "HARMONISED_TABLE", appeal_has_table):
            with mock.patch.object(AppealHasCuratedMipinsProcess, "OUTPUT_TABLE", output_table):
                inst = AppealHasCuratedMipinsProcess(spark)
                result = inst.run()
                assert_etl_result_successful(result)
                actual_table_data = spark.table(output_table)
                assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__appeal_has_curated_mipins_process__run__with_no_existing_data(self):
        """
        - Given I have harmonised appeal_has data
        - When I call AppealHasCuratedMipinsProcess.run
        - Then the data should be transformed and the appeals_has_curated_mipins table should be created
        """
        self.assert_curated_etl("t_ahcmp_r_wned")

    def test__appeal_has_curated_mipins_process__run__with_existing_data(self):
        """
        - Given I have harmonised appeal_has data and an existing appeals_has_curated_mipins table
        - When I call AppealHasCuratedMipinsProcess.run
        - Then the data should be transformed and the appeals_has_curated_mipins table should be overwritten
        """
        test_case = "t_ahcmp_r_wed"
        output_table = f"{test_case}_appeals_has_curated_mipins"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_data = spark.createDataFrame((_curated_row(caseId=1),), schema=_curated_schema())
        self.assert_curated_etl(test_case)
        self.write_existing_table(spark, existing_data, output_table, "odw_curated_db", "odw-curated", output_table, "overwrite")
        self.assert_curated_etl(test_case)
