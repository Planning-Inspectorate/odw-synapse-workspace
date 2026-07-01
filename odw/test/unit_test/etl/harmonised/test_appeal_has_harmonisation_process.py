import mock
from datetime import datetime
import pytest
import pyspark.sql.types as T
from pyspark.sql import Row
from pyspark.sql import functions as F
from odw.test.util.assertion import assert_dataframes_equal
from odw.core.etl.transformation.harmonised.appeal_has_harmonisation_process import AppealHasHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.skip(reason="Harmonisation logic not implemented yet")


def _appeal_has_schema():
    return T.StructType(
        [
            T.StructField("AppealsHasID", T.LongType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("caseId", T.StringType(), True),
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
            T.StructField("affectedListedBuildingNumbers", T.StringType(), True),
            T.StructField("appellantCostsAppliedFor", T.StringType(), True),
            T.StructField("lpaCostsAppliedFor", T.StringType(), True),
            T.StructField("typeOfPlanningApplication", T.StringType(), True),
            T.StructField("reasonForNeighbourVisits", T.StringType(), True),
            T.StructField("affectsScheduledMonument", T.StringType(), True),
            T.StructField("caseworkReason", T.StringType(), True),
            T.StructField("dateCostsReportDespatched", T.StringType(), True),
            T.StructField("dateNotRecoveredOrDerecovered", T.StringType(), True),
            T.StructField("dateRecovered", T.StringType(), True),
            T.StructField("designatedSitesNames", T.StringType(), True),
            T.StructField("didAppellantSubmitCompletePhotosAndPlans", T.StringType(), True),
            T.StructField("hasInfrastructureLevy", T.StringType(), True),
            T.StructField("hasLandownersPermission", T.StringType(), True),
            T.StructField("hasProtectedSpecies", T.StringType(), True),
            T.StructField("importantInformation", T.StringType(), True),
            T.StructField("infrastructureLevyAdoptedDate", T.StringType(), True),
            T.StructField("infrastructureLevyExpectedDate", T.StringType(), True),
            T.StructField("isAonbNationalLandscape", T.StringType(), True),
            T.StructField("isInfrastructureLevyFormallyAdopted", T.StringType(), True),
            T.StructField("isSiteInAreaOfSpecialControlAdverts", T.StringType(), True),
            T.StructField("advertDetails", T.StringType(), True),
            T.StructField("jurisdiction", T.StringType(), True),
            T.StructField("lpaProcedurePreference", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDetails", T.StringType(), True),
            T.StructField("lpaProcedurePreferenceDuration", T.StringType(), True),
            T.StructField("originalCaseDecisionDate", T.StringType(), True),
            T.StructField("redeterminedIndicator", T.StringType(), True),
            T.StructField("siteGridReferenceEasting", T.StringType(), True),
            T.StructField("siteGridReferenceNorthing", T.StringType(), True),
            T.StructField("targetDate", T.StringType(), True),
            T.StructField("wasApplicationRefusedDueToHighwayOrTraffic", T.StringType(), True),
            T.StructField("padsSapId", T.StringType(), True),
            T.StructField("migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("message_id", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _horizon_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("caseId", T.StringType(), True),
            T.StructField("caseStatus", T.StringType(), True),
            T.StructField("caseType", T.StringType(), True),
            T.StructField("caseProcedure", T.StringType(), True),
            T.StructField("lpaCode", T.StringType(), True),
            T.StructField("inspectorId", T.StringType(), True),
            T.StructField("allocationLevel", T.StringType(), True),
            T.StructField("allocationBand", T.StringType(), True),
            T.StructField("caseSpecialisms", T.StringType(), True),
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
            T.StructField("ingested_datetime", T.TimestampType(), True),
            T.StructField("expected_from", T.TimestampType(), True),
        ]
    )


def _s78_schema():
    return T.StructType(
        [
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _empty_df(spark, schema):
    return spark.createDataFrame([], schema)


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.harmonised.harmonsation_process.HarmonisationProcess.__init__",
        return_value=None,
    ):
        inst = AppealHasHarmonisationProcess(spark)

    inst.spark = spark
    return inst


def _base_sb_row(**overrides):
    row = {field.name: None for field in _appeal_has_schema()}
    row.update(
        {
            "AppealsHasID": None,
            "caseReference": "HAS-001",
            "caseId": "1001",
            "caseStatus": "submitted",
            "caseType": "HAS",
            "caseProcedure": "WR",
            "lpaCode": "LPA01",
            "caseSpecialisms": ["Advertisements"],
            "caseCreatedDate": "2025-01-01",
            "caseUpdatedDate": "2025-01-02",
            "siteAddressLine1": "1 Test Street",
            "siteAddressTown": "Test Town",
            "siteAreaSquareMetres": 1000.0,
            "migrated": "0",
            "ODTSourceSystem": "ODT",
            "IngestionDate": datetime(2025, 1, 1),
            "message_id": "msg-001",
            "SourceSystemID": "source-001",
            "ValidTo": None,
            "RowID": "",
            "IsActive": "Y",
        }
    )
    row.update(overrides)
    return Row(**row)


def _sb_df(spark, rows):
    return spark.createDataFrame(rows, _appeal_has_schema())


def _horizon_df(spark, rows):
    return spark.createDataFrame(rows, _horizon_schema())


def _s78_df(spark, rows=None):
    rows = rows or []
    return spark.createDataFrame(rows, _s78_schema())


def _source_data(spark, service_bus_data=None, horizon_data=None, appeal_s78_data=None):
    return {
        "service_bus_data": service_bus_data or _empty_df(spark, _appeal_has_schema()),
        "horizon_data": horizon_data or _empty_df(spark, _horizon_schema()),
        "appeal_s78_data": appeal_s78_data or _s78_df(spark),
    }


class TestRefAppealHasHarmonisationProcess(SparkTestCase):
    def test__appeal_has_harmonisation_process__process__unions_service_bus_and_horizon_and_aligns_schema_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="odt submitted",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 1, 1),
                )
            ],
        )

        horizon_data = _horizon_df(
            spark,
            [
                (
                    "HAS-002",
                    "1002",
                    "horizon submitted",
                    "HAS",
                    "LI",
                    "LPA02",
                    "INS-002",
                    "Level 2",
                    "Band B",
                    "Listed Building,Advertisements",
                    "2025-02-01",
                    "2025-02-02",
                    "2025-02-03",
                    "Valid appeal",
                    "2025-02-04",
                    "Linked",
                    "LEAD-002",
                    "2025-02-05",
                    "2025-02-06",
                    "2025-02-07",
                    "2025-02-08",
                    "Allowed",
                    "2025-02-09",
                    "APP-REF-002",
                    "2025-02-10",
                    "2025-02-11",
                    "Line 1",
                    "Line 2",
                    "Town",
                    "County",
                    "AA1 1AA",
                    2000.0,
                    50.0,
                    "Y",
                    "N",
                    "Development",
                    "Full Planning",
                    "2025-02-12",
                    "2025-02-13",
                    "2025-02-14",
                    "Important",
                    "Y",
                    "HAS",
                    "Written Reps",
                    "2025-02-15",
                    "N",
                    "123",
                    "456",
                    datetime(2025, 2, 1),
                    datetime(2025, 1, 31),
                )
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data, horizon_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]
        actual_df = df.select(
            "caseReference",
            "ODTSourceSystem",
            "caseSpecialisms",
            "migrated",
            "IsActive",
            "AppealsHasID",
        )

        expected_df = spark.createDataFrame(
            [
                ("HAS-001", "ODT", ["Advertisements"], "0", "Y", None),
                ("HAS-002", "HORIZON", ["Listed Building", "Advertisements"], "0", "Y", 1),
            ],
            actual_df.schema,
        )

        assert df.count() == 2
        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__appeal_has_harmonisation_process__process__builds_scd2_history_for_changed_state_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="submitted", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-001", caseStatus="valid", IngestionDate=datetime(2025, 2, 1)),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]
        actual_df = df.select(
            "caseStatus",
            "IsActive",
            "ValidTo",
            "AppealsHasID",
        )

        expected_df = spark.createDataFrame(
            [
                ("submitted", "N", datetime(2025, 2, 1), 1),
                ("valid", "Y", None, 2),
            ],
            actual_df.schema,
        )

        assert df.count() == 2
        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__appeal_has_harmonisation_process__process__drops_duplicate_unchanged_state_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="submitted", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-001", caseStatus="submitted", IngestionDate=datetime(2025, 2, 1)),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]

        assert df.count() == 1
        assert df.collect()[0]["caseStatus"] == "submitted"
        assert df.collect()[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__appeal_has_harmonisation_process__process__filters_null_case_reference_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference=None, caseStatus="bad row", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-001", caseStatus="good row", IngestionDate=datetime(2025, 1, 1)),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]

        assert df.count() == 1
        assert df.collect()[0]["caseReference"] == "HAS-001"
        assert result.metadata.insert_count == 1

    def test__appeal_has_harmonisation_process__process__uses_odt_over_horizon_when_timestamp_ties_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="odt state",
                    ODTSourceSystem="ODT",
                    IngestionDate=datetime(2025, 1, 1),
                )
            ],
        )

        horizon_data = _horizon_df(
            spark,
            [
                (
                    "HAS-001",
                    "1001",
                    "horizon state",
                    "HAS",
                    "WR",
                    "LPA01",
                    None,
                    None,
                    None,
                    "Advertisements",
                    "2025-01-01",
                    "2025-01-01",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    datetime(2025, 1, 1),
                    datetime(2025, 1, 1),
                )
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_source_data(spark, service_bus_data, horizon_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].orderBy("IngestionDate", F.desc("ODTSourceSystem"))
        rows = df.collect()

        assert df.count() == 2
        assert rows[0]["ODTSourceSystem"] == "ODT"
        assert rows[0]["ValidTo"] == datetime(2025, 1, 1, 0, 0, 0, 1)
        assert rows[0]["IsActive"] == "N"
        assert rows[1]["ODTSourceSystem"] == "HORIZON"
        assert rows[1]["IsActive"] == "Y"

    def test__appeal_has_harmonisation_process__process__later_s78_timestamp_closes_active_has_row_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="has active", IngestionDate=datetime(2025, 1, 1)),
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-001", datetime(2025, 2, 1), None, "Y"),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_source_data(spark, service_bus_data, appeal_s78_data=appeal_s78_data))

        row = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].collect()[0]

        assert row["caseReference"] == "HAS-001"
        assert row["ValidTo"] == datetime(2025, 2, 1)
        assert row["IsActive"] == "N"

    def test__appeal_has_harmonisation_process__process__ignores_earlier_s78_timestamp_for_has_valid_to_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="has active", IngestionDate=datetime(2025, 2, 1)),
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-001", datetime(2025, 1, 1), None, "Y"),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_source_data(spark, service_bus_data, appeal_s78_data=appeal_s78_data))

        row = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].collect()[0]

        assert row["caseReference"] == "HAS-001"
        assert row["ValidTo"] is None
        assert row["IsActive"] == "Y"

    def test__appeal_has_harmonisation_process__process__keeps_expected_output_column_order_and_write_config(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="submitted", IngestionDate=datetime(2025, 1, 1)),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data))

        write_config = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]
        df = write_config["data"]

        assert df.columns == [field.name for field in _appeal_has_schema()]
        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1

    def test__appeal_has_harmonisation_process__process__uses_expected_from_when_horizon_ingested_datetime_is_missing(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = _horizon_df(
            spark,
            [
                (
                    "HAS-001",
                    "1001",
                    "horizon only",
                    "HAS",
                    "WR",
                    "LPA01",
                    None,
                    None,
                    None,
                    "Advertisements",
                    "2025-01-01",
                    "2025-01-02",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    datetime(2025, 1, 5),
                )
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_source_data(spark, horizon_data=horizon_data))

        row = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].collect()[0]

        assert row["caseReference"] == "HAS-001"
        assert row["IngestionDate"] == datetime(2025, 1, 5)
        assert row["ODTSourceSystem"] == "HORIZON"

    def test__appeal_has_harmonisation_process__process__drops_exact_duplicate_stage_rows_like_legacy_distinct(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate_row = _base_sb_row(
            caseReference="HAS-001",
            caseStatus="submitted",
            IngestionDate=datetime(2025, 1, 1),
        )

        service_bus_data = _sb_df(spark, [duplicate_row, duplicate_row])

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data=service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_has_harmonisation_process__process__collapses_exact_duplicate_state_within_same_key_timestamp_source(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="submitted",
                    RowID="a-row",
                    IngestionDate=datetime(2025, 1, 1),
                ),
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="submitted",
                    RowID="b-row",
                    IngestionDate=datetime(2025, 1, 1),
                ),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data=service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_has_harmonisation_process__process__same_timestamp_changed_states_get_microsecond_valid_to_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="first state",
                    RowID="a-row",
                    IngestionDate=datetime(2025, 1, 1),
                ),
                _base_sb_row(
                    caseReference="HAS-001",
                    caseStatus="second state",
                    RowID="b-row",
                    IngestionDate=datetime(2025, 1, 1),
                ),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_source_data(spark, service_bus_data=service_bus_data))

        rows = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].orderBy("RowID").collect()

        assert len(rows) == 2
        assert rows[0]["RowID"] == "a-row"
        assert rows[0]["ValidTo"] == datetime(2025, 1, 1, 0, 0, 0, 1)
        assert rows[0]["IsActive"] == "N"
        assert rows[1]["RowID"] == "b-row"
        assert rows[1]["ValidTo"] is None
        assert rows[1]["IsActive"] == "Y"
        assert result.metadata.insert_count == 2

    def test__appeal_has_harmonisation_process__process__keeps_only_one_active_row_per_case_reference(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="submitted", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-001", caseStatus="valid", IngestionDate=datetime(2025, 2, 1)),
                _base_sb_row(caseReference="HAS-002", caseStatus="submitted", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-002", caseStatus="complete", IngestionDate=datetime(2025, 3, 1)),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_source_data(spark, service_bus_data=service_bus_data))

        df = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"]

        active_counts = {row["caseReference"]: row["count"] for row in df.where(F.col("IsActive") == "Y").groupBy("caseReference").count().collect()}

        assert active_counts == {"HAS-001": 1, "HAS-002": 1}

    def test__appeal_has_harmonisation_process__process__s78_only_closes_has_when_s78_timestamp_is_strictly_after_has_ingestion(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="has active", IngestionDate=datetime(2025, 2, 1)),
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-001", datetime(2025, 2, 1), None, "Y"),
                ("HAS-001", datetime(2025, 3, 1), None, "Y"),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(
            _source_data(
                spark,
                service_bus_data=service_bus_data,
                appeal_s78_data=appeal_s78_data,
            )
        )

        row = data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].collect()[0]

        assert row["ValidTo"] == datetime(2025, 3, 1)
        assert row["IsActive"] == "N"

    def test__appeal_has_harmonisation_process__process__does_not_close_already_inactive_has_row_again_when_s78_exists(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = _sb_df(
            spark,
            [
                _base_sb_row(caseReference="HAS-001", caseStatus="old has", IngestionDate=datetime(2025, 1, 1)),
                _base_sb_row(caseReference="HAS-001", caseStatus="new has", IngestionDate=datetime(2025, 2, 1)),
            ],
        )

        appeal_s78_data = _s78_df(
            spark,
            [
                ("HAS-001", datetime(2025, 3, 1), None, "Y"),
            ],
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(
            _source_data(
                spark,
                service_bus_data=service_bus_data,
                appeal_s78_data=appeal_s78_data,
            )
        )

        rows = {row["caseStatus"]: row for row in data_to_write[f"odw_harmonised_db.{inst.OUTPUT_TABLE}"]["data"].collect()}

        assert rows["old has"]["ValidTo"] == datetime(2025, 2, 1)
        assert rows["old has"]["IsActive"] == "N"
        assert rows["new has"]["ValidTo"] == datetime(2025, 3, 1)
        assert rows["new has"]["IsActive"] == "N"
