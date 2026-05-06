from datetime import datetime
from decimal import Decimal
import pytest
import pyspark.sql.types as T
from pyspark.sql import Row
from odw.core.etl.transformation.curated.appeal_has_curated_mipins_process import AppealHasCuratedMipinsProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curated MIPINS logic not implemented yet")


OUTPUT_COLUMNS = [
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
    "affectedListedBuildingNumbers",
    "appellantCostsAppliedFor",
    "lpaCostsAppliedFor",
    "typeOfPlanningApplication",
    "hasLandownersPermission",
    "wasApplicationRefusedDueToHighwayOrTraffic",
    "didAppellantSubmitCompletePhotosAndPlans",
    "isSiteInAreaOfSpecialControlAdverts",
    "advertDetails",
    "padsSapId",
    "applicationDecisionDueDate",
    "IngestionDate",
    "ValidTo",
    "IsActive",
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
            "caseWithdrawnDate": None,
            "caseTransferredDate": None,
            "transferredCaseClosedDate": None,
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
    return Row(**row)


def _df(spark, rows):
    return spark.createDataFrame(rows, _harmonised_schema())


def _source_data(harmonised_data):
    return {"harmonised_data": harmonised_data}


class TestAppealHasCuratedMipinsProcess(SparkTestCase):
    def test__appeal_has_curated_mipins_process__process__projects_casts_filters_and_converts_timestamps_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(spark, [_base_row()])

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]
        row = df.collect()[0]

        assert df.columns == OUTPUT_COLUMNS
        assert df.count() == 1

        assert row["caseId"] == 7000001
        assert row["caseReference"] == "7000001"
        assert row["allocationBand"] == Decimal("7")
        assert row["caseValidationIncompleteDetails"] == "[Missing plan]"
        assert row["lpaQuestionnaireValidationDetails"] == "[Accepted]"
        assert row["enforcementNotice"] is True
        assert row["siteAreaSquareMetres"] is True
        assert row["floorSpaceSquareMetres"] == Decimal("123")
        assert row["isCorrectAppealType"] is True
        assert row["isGreenBelt"] is False
        assert row["inConservationArea"] is True
        assert row["ownsAllLand"] is False
        assert row["ownsSomeLand"] is True
        assert row["advertisedAppeal"] is True
        assert row["ownersInformed"] is False
        assert row["changedDevelopmentDescription"] is True
        assert row["neighbouringSiteAddresses"] == "[Neighbour address]"
        assert row["appellantCostsAppliedFor"] is True
        assert row["lpaCostsAppliedFor"] is False

        assert row["caseSubmittedDate"] == datetime(2025, 6, 1, 13, 0)
        assert row["IngestionDate"] == datetime(2025, 6, 1, 13, 0)
        assert row["ValidTo"] == datetime(2025, 6, 1, 13, 0)

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert "partition_by" not in write_config
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

    def test__appeal_has_curated_mipins_process__process__applies_legacy_source_and_reference_filters(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(caseReference="7000001", lpaCode="Q1234", ODTSourceSystem="ODT"),
                _base_row(caseReference="7000002", lpaCode="Q9999", ODTSourceSystem="ODT"),
                _base_row(caseReference="7000003", lpaCode="Q1111", ODTSourceSystem="ODT"),
                _base_row(caseReference="5000000", lpaCode="Q1234", ODTSourceSystem="ODT"),
                _base_row(caseReference="7000004", lpaCode="Q1234", ODTSourceSystem="HORIZON"),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert df.count() == 1
        assert rows[0]["caseReference"] == "7000001"
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__does_not_filter_on_is_active_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(caseReference="7000001", IsActive="N"),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["caseReference"] == "7000001"
        assert row["IsActive"] == "N"
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__filters_rows_with_pre_1900_dates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(caseReference="7000001"),
                _base_row(caseReference="7000002", caseCreatedDate=datetime(1899, 12, 31)),
                _base_row(caseReference="7000003", applicationDecisionDueDate=datetime(1899, 12, 31)),
                _base_row(caseReference="7000004", IngestionDate=datetime(1899, 12, 31)),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        rows = data_to_write[inst.OUTPUT_TABLE]["data"].collect()

        assert len(rows) == 1
        assert rows[0]["caseReference"] == "7000001"
        assert result.metadata.insert_count == 1

    @pytest.mark.parametrize(
        "date_column",
        [
            "caseSubmittedDate",
            "caseCreatedDate",
            "caseUpdatedDate",
            "caseValidDate",
            "caseValidationDate",
            "caseExtensionDate",
            "caseStartedDate",
            "casePublishedDate",
            "lpaQuestionnaireDueDate",
            "lpaQuestionnaireSubmittedDate",
            "lpaQuestionnaireCreatedDate",
            "lpaQuestionnairePublishedDate",
            "lpaQuestionnaireValidationOutcomeDate",
            "caseWithdrawnDate",
            "caseTransferredDate",
            "transferredCaseClosedDate",
            "caseDecisionOutcomeDate",
            "caseDecisionPublishedDate",
            "caseCompletedDate",
            "applicationDate",
            "applicationDecisionDate",
            "caseSubmissionDueDate",
            "applicationDecisionDueDate",
            "IngestionDate",
        ],
    )
    def test__appeal_has_curated_mipins_process__process__filters_each_pre_1900_date_column_like_legacy(
        self,
        date_column,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(caseReference="7000001"),
                _base_row(
                    caseReference="7000002",
                    **{date_column: datetime(1899, 12, 31)},
                ),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        rows = data_to_write[inst.OUTPUT_TABLE]["data"].collect()

        assert len(rows) == 1
        assert rows[0]["caseReference"] == "7000001"
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__keeps_1900_boundary_dates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(
                    caseReference="7000001",
                    caseCreatedDate=datetime(1900, 1, 1),
                    applicationDecisionDueDate=datetime(1900, 1, 1),
                    IngestionDate=datetime(1900, 1, 1),
                ),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__preserves_null_dates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = _df(
            spark,
            [
                _base_row(
                    caseReference="7000001",
                    caseCreatedDate=None,
                    applicationDecisionDueDate=None,
                    ValidTo=None,
                ),
            ],
        )

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["caseCreatedDate"] is None
        assert row["applicationDecisionDueDate"] is None
        assert row["ValidTo"] is None
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__drops_exact_duplicates_like_legacy_select_distinct(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate = _base_row(caseReference="7000001")
        source_df = _df(spark, [duplicate, duplicate])

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_has_curated_mipins_process__process__empty_input_writes_empty_output_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame([], _harmonised_schema())

        inst = AppealHasCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(_source_data(source_df))

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 0
        assert df.columns == OUTPUT_COLUMNS
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 0
