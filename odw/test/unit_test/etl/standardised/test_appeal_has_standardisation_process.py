import mock
import pytest
import pyspark.sql.types as T
from odw.core.etl.transformation.standardised.appeal_has_standardisation_process import AppealHasStandardisationProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Standardisation logic not implemented yet")


def _metadata_schema(extra_fields):
    return T.StructType(
        extra_fields
        + [
            T.StructField("expected_from", T.StringType(), True),
            T.StructField("modified_datetime", T.StringType(), True),
            T.StructField("ingested_datetime", T.StringType(), True),
            T.StructField("file_id", T.IntegerType(), True),
        ]
    )


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.standardised.standardisation_process.StandardisationProcess.__init__",
        return_value=None,
    ):
        inst = AppealHasStandardisationProcess(spark)

    inst.spark = spark
    return inst


def _empty_df(spark, schema):
    return spark.createDataFrame([], schema)


def _minimal_hzn_cases_has_df(spark):
    return spark.createDataFrame(
        [
            (
                "HAS-001",
                1001,
                "HAS",
                "2025-01-10",
                "2025-01-11",
                "2025-01-12",
                "2025-01-13",
                1,
            ),
        ],
        _metadata_schema(
            [
                T.StructField("caseuniqueid", T.StringType(), True),
                T.StructField("casenodeid", T.IntegerType(), True),
                T.StructField("abbreviation", T.StringType(), True),
                T.StructField("modifydate", T.StringType(), True),
            ]
        ),
    )


def _minimal_source_data(spark, hzn_cases_has_df):
    return {
        "hzn_cases_has_df": hzn_cases_has_df,
        "cases_specialism_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casereference", T.StringType(), True),
                    T.StructField("casespecialism", T.StringType(), True),
                ]
            ),
        ),
        "vw_case_dates_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("receiptdate", T.StringType(), True),
                    T.StructField("appealdocscomplete", T.StringType(), True),
                    T.StructField("startdate", T.StringType(), True),
                    T.StructField("appealwithdrawndate", T.StringType(), True),
                    T.StructField("casedecisiondate", T.StringType(), True),
                    T.StructField("datenotrecoveredorderecovered", T.StringType(), True),
                    T.StructField("daterecovered", T.StringType(), True),
                    T.StructField("originalcasedecisiondate", T.StringType(), True),
                ]
            ),
        ),
        "CaseDocumentDatesDates_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("questionnairedue", T.StringType(), True),
                    T.StructField("questionnairereceived", T.StringType(), True),
                    T.StructField("interestedpartyrepsduedate", T.StringType(), True),
                    T.StructField("proofsdue", T.StringType(), True),
                ]
            ),
        ),
        "CaseSiteStrings_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("siteviewablefromroad", T.StringType(), True),
                    T.StructField("addressline1", T.StringType(), True),
                    T.StructField("addressline2", T.StringType(), True),
                    T.StructField("town", T.StringType(), True),
                    T.StructField("county", T.StringType(), True),
                    T.StructField("postcode", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                ]
            ),
        ),
        "vw_AddAdditionalData_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("floorspaceinsquaremetres", T.DoubleType(), True),
                    T.StructField("costsappliedforindicator", T.StringType(), True),
                    T.StructField("procedureappellant", T.StringType(), True),
                    T.StructField("isthesitewithinanaonb", T.StringType(), True),
                    T.StructField("procedurelpa", T.StringType(), True),
                    T.StructField("inspectorneedtoentersite", T.StringType(), True),
                    T.StructField("sitegridreferenceeasting", T.StringType(), True),
                    T.StructField("sitegridreferencenorthing", T.StringType(), True),
                    T.StructField("sitewithinsssi", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                ]
            ),
        ),
        "vw_AdditionalFields_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("importantinformation", T.StringType(), True),
                ]
            ),
        ),
        "horizon_advert_attributes_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("advertAttributeKey", T.StringType(), True),
                    T.StructField("advertType", T.StringType(), True),
                    T.StructField("setRowNumber", T.IntegerType(), True),
                    T.StructField("publicSafetyLpaIndicator", T.StringType(), True),
                    T.StructField("amenityLPAIndicator", T.StringType(), True),
                    T.StructField("publicSafetyGroundOutcome", T.StringType(), True),
                    T.StructField("amenityGroundOutcome", T.StringType(), True),
                    T.StructField("advertInPosition", T.StringType(), True),
                    T.StructField("siteOnHighwayLand", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_curr_TypeOfLevel_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("name", T.StringType(), True),
                    T.StructField("band", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_SpecialistCaseDates_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("datecostsreportdespatched", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_PlanningAppStrings_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("lpaapplicationreference", T.StringType(), True),
                    T.StructField("planningapplicationtype", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_PlanningAppDates_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("dateofapplication", T.StringType(), True),
                    T.StructField("dateoflpadecision", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_LeadCase_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("leadcasenodeid", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_CaseStrings_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("processingstate", T.StringType(), True),
                    T.StructField("lpacode", T.StringType(), True),
                    T.StructField("linkedstatus", T.StringType(), True),
                    T.StructField("decision", T.StringType(), True),
                    T.StructField("jurisdiction", T.StringType(), True),
                    T.StructField("redetermined", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_CaseInfo_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validity", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_CaseDates_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validitystatusdate", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_AppealsAdditionalData_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("developmentorallegation", T.StringType(), True),
                    T.StructField("appellantcommentssubmitted", T.StringType(), True),
                    T.StructField("appellantstatementsubmitted", T.StringType(), True),
                    T.StructField("lpacommentssubmitted", T.StringType(), True),
                    T.StructField("lpaproofssubmitted", T.StringType(), True),
                    T.StructField("lpastatementsubmitted", T.StringType(), True),
                    T.StructField("sitenoticesent", T.StringType(), True),
                    T.StructField("statementsdue", T.StringType(), True),
                    T.StructField("areaofsiteinhectares", T.DoubleType(), True),
                ]
            ),
        ),
        "Horizon_vw_BIS_CaseSiteCategoryAdditionalStr_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("AppealRefNumber", T.StringType(), True),
                    T.StructField("SiteWithinAGreenBelt", T.StringType(), True),
                    T.StructField("InCARelatesToCA", T.StringType(), True),
                ]
            ),
        ),
        "Horizon_ODW_vw_Inspector_Cases_df": _empty_df(
            spark,
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("contactid", T.StringType(), True),
                ]
            ),
        ),
    }


class TestRefAppealHasStandardisationProcess(SparkTestCase):
    def test__appeal_has_standardisation_process__load_data__loads_expected_source_tables(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)

        expected_keys = {
            "hzn_cases_has_df",
            "cases_specialism_df",
            "vw_case_dates_df",
            "CaseDocumentDatesDates_df",
            "CaseSiteStrings_df",
            "vw_AddAdditionalData_df",
            "vw_AdditionalFields_df",
            "horizon_advert_attributes_df",
            "Horizon_vw_curr_TypeOfLevel_df",
            "Horizon_vw_BIS_SpecialistCaseDates_df",
            "Horizon_vw_BIS_PlanningAppStrings_df",
            "Horizon_vw_BIS_PlanningAppDates_df",
            "Horizon_vw_BIS_LeadCase_df",
            "Horizon_vw_BIS_CaseStrings_df",
            "Horizon_vw_BIS_CaseInfo_df",
            "Horizon_vw_BIS_CaseDates_df",
            "Horizon_vw_BIS_AppealsAdditionalData_df",
            "Horizon_vw_BIS_CaseSiteCategoryAdditionalStr_df",
            "Horizon_ODW_vw_Inspector_Cases_df",
        }

        mock_df = _empty_df(
            spark,
            T.StructType([T.StructField("id", T.StringType(), True)]),
        )

        with mock.patch.object(spark, "sql", return_value=mock_df) as mock_sql:
            source_data = inst.load_data()

        assert set(source_data.keys()) == expected_keys

        for key in expected_keys:
            assert source_data[key] is mock_df

        assert mock_sql.call_count == len(expected_keys)

    def test__appeal_has_standardisation_process__process__maps_base_has_columns_and_uses_overwrite(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_minimal_source_data(spark, _minimal_hzn_cases_has_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        actual_df = df.select(
            "caseReference",
            "caseId",
            "caseType",
            "caseUpdatedDate",
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    1001,
                    "HAS",
                    "2025-01-10",
                ),
            ],
            T.StructType(
                [
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("caseId", T.IntegerType(), True),
                    T.StructField("caseType", T.StringType(), True),
                    T.StructField("caseUpdatedDate", T.StringType(), True),
                ]
            ),
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_has_standardisation_process__process__keeps_latest_has_row_per_case_reference_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hzn_cases_has_df = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    1001,
                    "OLD",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-001",
                    1002,
                    "NEW",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("abbreviation", T.StringType(), True),
                    T.StructField("modifydate", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(_minimal_source_data(spark, hzn_cases_has_df))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.count() == 1
        assert row["caseReference"] == "HAS-001"
        assert row["caseId"] == 1002
        assert row["caseType"] == "NEW"
        assert result.metadata.insert_count == 1

    def test__appeal_has_standardisation_process__process__aggregates_case_specialisms_sorted_and_distinct(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))
        source_data["cases_specialism_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    "Listed Building",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-001",
                    "Advertisements",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-001",
                    "Advertisements",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                ("HAS-001", "", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-001", None, "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("casereference", T.StringType(), True),
                    T.StructField("casespecialism", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["caseSpecialisms"] == "Advertisements, Listed Building"

    def test__appeal_has_standardisation_process__process__normalises_case_procedure_from_appellant_and_lpa_aliases(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hzn_cases_has_df = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    1001,
                    "HAS",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    1,
                ),
                (
                    "HAS-002",
                    1002,
                    "HAS",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    1,
                ),
                (
                    "HAS-003",
                    1003,
                    "HAS",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    1,
                ),
                (
                    "HAS-004",
                    1004,
                    "HAS",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("abbreviation", T.StringType(), True),
                    T.StructField("modifydate", T.StringType(), True),
                ]
            ),
        )

        source_data = _minimal_source_data(spark, hzn_cases_has_df)
        source_data["vw_AddAdditionalData_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    None,
                    None,
                    "Written Reps",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-002",
                    None,
                    None,
                    "Hearing",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-003",
                    None,
                    None,
                    None,
                    None,
                    "Public Inquiry",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-004",
                    None,
                    None,
                    "Not Known",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("floorspaceinsquaremetres", T.DoubleType(), True),
                    T.StructField("costsappliedforindicator", T.StringType(), True),
                    T.StructField("procedureappellant", T.StringType(), True),
                    T.StructField("isthesitewithinanaonb", T.StringType(), True),
                    T.StructField("procedurelpa", T.StringType(), True),
                    T.StructField("inspectorneedtoentersite", T.StringType(), True),
                    T.StructField("sitegridreferenceeasting", T.StringType(), True),
                    T.StructField("sitegridreferencenorthing", T.StringType(), True),
                    T.StructField("sitewithinsssi", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        rows = {row["caseReference"]: row["caseProcedure"] for row in data_to_write[inst.OUTPUT_TABLE]["data"].collect()}

        assert rows["HAS-001"] == "WR"
        assert rows["HAS-002"] == "IH"
        assert rows["HAS-003"] == "LI"
        assert rows["HAS-004"] is None

    def test__appeal_has_standardisation_process__process__joins_latest_related_records_without_fanout(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))
        source_data["CaseSiteStrings_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    None,
                    "Old line 1",
                    "Old line 2",
                    "Old Town",
                    "Old County",
                    "OLD",
                    "UK",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    1001,
                    None,
                    "New line 1",
                    "New line 2",
                    "New Town",
                    "New County",
                    "NEW",
                    "UK",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("siteviewablefromroad", T.StringType(), True),
                    T.StructField("addressline1", T.StringType(), True),
                    T.StructField("addressline2", T.StringType(), True),
                    T.StructField("town", T.StringType(), True),
                    T.StructField("county", T.StringType(), True),
                    T.StructField("postcode", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.count() == 1
        assert row["siteAddressLine1"] == "New line 1"
        assert row["siteAddressLine2"] == "New line 2"
        assert row["siteAddressTown"] == "New Town"
        assert row["siteAddressPostcode"] == "NEW"
        assert result.metadata.insert_count == 1

    def test__appeal_has_standardisation_process__process__maps_group_a_case_and_planning_fields(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["Horizon_vw_BIS_PlanningAppStrings_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "APP-REF-001",
                    "Full Planning",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                )
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("lpaapplicationreference", T.StringType(), True),
                    T.StructField("planningapplicationtype", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_PlanningAppDates_df"] = spark.createDataFrame(
            [(1001, "2025-01-02", "2025-01-03", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("dateofapplication", T.StringType(), True),
                    T.StructField("dateoflpadecision", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseStrings_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "Valid",
                    "LPA01",
                    "Linked",
                    "Allowed",
                    "HAS",
                    "N",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                )
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("processingstate", T.StringType(), True),
                    T.StructField("lpacode", T.StringType(), True),
                    T.StructField("linkedstatus", T.StringType(), True),
                    T.StructField("decision", T.StringType(), True),
                    T.StructField("jurisdiction", T.StringType(), True),
                    T.StructField("redetermined", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["applicationReference"] == "APP-REF-001"
        assert row["typeOfPlanningApplication"] == "Full Planning"
        assert row["applicationDate"] == "2025-01-02"
        assert row["applicationDecisionDate"] == "2025-01-03"
        assert row["caseStatus"] == "Valid"
        assert row["lpaCode"] == "LPA01"
        assert row["linkedCaseStatus"] == "Linked"
        assert row["caseDecisionOutcome"] == "Allowed"
        assert row["jurisdiction"] == "HAS"
        assert row["redeterminedIndicator"] == "N"

    def test__appeal_has_standardisation_process__process__maps_all_legacy_date_fields(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["vw_case_dates_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "2025-01-01",
                    "2025-01-02",
                    "2025-01-03",
                    "2025-01-04",
                    "2025-01-05",
                    "2025-01-06",
                    "2025-01-07",
                    "2025-01-08",
                    "2025-01-09",
                    "2025-01-09",
                    "2025-01-09",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("receiptdate", T.StringType(), True),
                    T.StructField("appealdocscomplete", T.StringType(), True),
                    T.StructField("startdate", T.StringType(), True),
                    T.StructField("appealwithdrawndate", T.StringType(), True),
                    T.StructField("casedecisiondate", T.StringType(), True),
                    T.StructField("datenotrecoveredorderecovered", T.StringType(), True),
                    T.StructField("daterecovered", T.StringType(), True),
                    T.StructField("originalcasedecisiondate", T.StringType(), True),
                ]
            ),
        )

        source_data["CaseDocumentDatesDates_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "2025-02-01",
                    "2025-02-02",
                    "2025-02-03",
                    "2025-02-04",
                    "2025-02-05",
                    "2025-02-05",
                    "2025-02-05",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("questionnairedue", T.StringType(), True),
                    T.StructField("questionnairereceived", T.StringType(), True),
                    T.StructField("interestedpartyrepsduedate", T.StringType(), True),
                    T.StructField("proofsdue", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseDates_df"] = spark.createDataFrame(
            [("HAS-001", "2025-03-01", "2025-03-02", "2025-03-02", "2025-03-02", 1)],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validitystatusdate", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_SpecialistCaseDates_df"] = spark.createDataFrame(
            [("HAS-001", "2025-04-01", "2025-04-02", "2025-04-02", "2025-04-02", 1)],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("datecostsreportdespatched", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["caseCreatedDate"] == "2025-01-01"
        assert row["caseStartedDate"] == "2025-01-03"
        assert row["caseWithdrawnDate"] == "2025-01-04"
        assert row["caseDecisionOutcomeDate"] == "2025-01-05"
        assert row["caseCompletedDate"] == "2025-01-05"
        assert row["dateNotRecoveredOrDerecovered"] == "2025-01-06"
        assert row["dateRecovered"] == "2025-01-07"
        assert row["originalCaseDecisionDate"] == "2025-01-08"
        assert row["lpaQuestionnaireDueDate"] == "2025-02-01"
        assert row["lpaQuestionnaireSubmittedDate"] == "2025-02-02"
        assert row["caseValidationDate"] == "2025-03-01"
        assert row["dateCostsReportDespatched"] == "2025-04-01"

    def test__appeal_has_standardisation_process__process__maps_validation_inspector_geo_lead_case_and_important_info(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["vw_AdditionalFields_df"] = spark.createDataFrame(
            [("HAS-001", "Important info text", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("importantinformation", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_LeadCase_df"] = spark.createDataFrame(
            [(1001, "LEAD-001", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("leadcasenodeid", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseInfo_df"] = spark.createDataFrame(
            [("HAS-001", "Valid appeal", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validity", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseSiteCategoryAdditionalStr_df"] = spark.createDataFrame(
            [("HAS-001", "Y", "N", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("AppealRefNumber", T.StringType(), True),
                    T.StructField("SiteWithinAGreenBelt", T.StringType(), True),
                    T.StructField("InCARelatesToCA", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_ODW_vw_Inspector_Cases_df"] = spark.createDataFrame(
            [("HAS-001", "INSPECTOR-001", "2025-01-01", "2025-01-01", "2025-01-01", 1)],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("contactid", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["importantinformation"] == "Important info text"
        assert row["leadCaseReference"] == "LEAD-001"
        assert row["caseValidationOutcome"] == "Valid appeal"
        assert row["isGreenBelt"] == "Y"
        assert row["inConservationArea"] == "N"
        assert row["inspectorId"] == "INSPECTOR-001"

    def test__appeal_has_standardisation_process__process__maps_site_area_floor_space_and_grid_references(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["vw_AddAdditionalData_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    50.0,
                    None,
                    None,
                    "Y",
                    None,
                    None,
                    "123456",
                    "654321",
                    "N",
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("floorspaceinsquaremetres", T.DoubleType(), True),
                    T.StructField("costsappliedforindicator", T.StringType(), True),
                    T.StructField("procedureappellant", T.StringType(), True),
                    T.StructField("isthesitewithinanaonb", T.StringType(), True),
                    T.StructField("procedurelpa", T.StringType(), True),
                    T.StructField("inspectorneedtoentersite", T.StringType(), True),
                    T.StructField("sitegridreferenceeasting", T.StringType(), True),
                    T.StructField("sitegridreferencenorthing", T.StringType(), True),
                    T.StructField("sitewithinsssi", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_AppealsAdditionalData_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    1.25,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                )
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("developmentorallegation", T.StringType(), True),
                    T.StructField("appellantcommentssubmitted", T.StringType(), True),
                    T.StructField("appellantstatementsubmitted", T.StringType(), True),
                    T.StructField("lpacommentssubmitted", T.StringType(), True),
                    T.StructField("lpaproofssubmitted", T.StringType(), True),
                    T.StructField("lpastatementsubmitted", T.StringType(), True),
                    T.StructField("sitenoticesent", T.StringType(), True),
                    T.StructField("statementsdue", T.StringType(), True),
                    T.StructField("areaofsiteinhectares", T.DoubleType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["siteAreaSquareMetres"] == 12500.0
        assert row["floorSpaceSquareMetres"] == 50.0
        assert row["siteGridReferenceEasting"] == "123456"
        assert row["siteGridReferenceNorthing"] == "654321"
        assert row["isAonbNationalLandscape"] == "Y"

    def test__appeal_has_standardisation_process__process__maps_advert_fields(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["horizon_advert_attributes_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    "ATTR-001",
                    "Poster",
                    2,
                    "Y",
                    "N",
                    "Public safety outcome",
                    "Amenity outcome",
                    "Y",
                    "N",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("advertAttributeKey", T.StringType(), True),
                    T.StructField("advertType", T.StringType(), True),
                    T.StructField("setRowNumber", T.IntegerType(), True),
                    T.StructField("publicSafetyLpaIndicator", T.StringType(), True),
                    T.StructField("amenityLPAIndicator", T.StringType(), True),
                    T.StructField("publicSafetyGroundOutcome", T.StringType(), True),
                    T.StructField("amenityGroundOutcome", T.StringType(), True),
                    T.StructField("advertInPosition", T.StringType(), True),
                    T.StructField("siteOnHighwayLand", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["advertAttributeKey"] == "ATTR-001"
        assert row["advertType"] == "Poster"
        assert row["advertSetRowNumber"] == 2
        assert row["publicSafetyLpaIndicator"] == "Y"
        assert row["amenityLpaIndicator"] == "N"
        assert row["publicSafetyGroundOutcome"] == "Public safety outcome"
        assert row["amenityGroundOutcome"] == "Amenity outcome"
        assert row["advertInPosition"] == "Y"
        assert row["siteOnHighwayLand"] == "N"

    def test__appeal_has_standardisation_process__process__keeps_expected_final_column_order(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(_minimal_source_data(spark, _minimal_hzn_cases_has_df(spark)))

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "caseReference",
            "caseId",
            "caseType",
            "caseStatus",
            "applicationReference",
            "typeOfPlanningApplication",
            "applicationDate",
            "applicationDecisionDate",
            "caseDecisionOutcome",
            "jurisdiction",
            "redeterminedIndicator",
            "lpaCode",
            "linkedCaseStatus",
            "leadCaseReference",
            "siteAddressTown",
            "siteAddressPostcode",
            "siteAddressLine2",
            "siteAddressLine1",
            "siteAddressCounty",
            "isGreenBelt",
            "inConservationArea",
            "siteGridReferenceNorthing",
            "siteGridReferenceEasting",
            "isAonbNationalLandscape",
            "caseValidationOutcome",
            "caseValidationDate",
            "dateCostsReportDespatched",
            "lpaQuestionnaireSubmittedDate",
            "lpaQuestionnaireDueDate",
            "originalCaseDecisionDate",
            "dateRecovered",
            "dateNotRecoveredOrDerecovered",
            "caseWithdrawnDate",
            "caseStartedDate",
            "caseDecisionOutcomeDate",
            "caseCreatedDate",
            "caseCompletedDate",
            "siteAreaSquareMetres",
            "floorSpaceSquareMetres",
            "originalDevelopmentDescription",
            "importantinformation",
            "lpaProcedurePreference",
            "caseProcedure",
            "inspectorId",
            "allocationLevel",
            "allocationBand",
            "caseSpecialisms",
            "caseUpdatedDate",
            "advertAttributeKey",
            "advertType",
            "advertSetRowNumber",
            "publicSafetyLpaIndicator",
            "amenityLpaIndicator",
            "publicSafetyGroundOutcome",
            "amenityGroundOutcome",
            "advertInPosition",
            "siteOnHighwayLand",
        ]

    def test__appeal_has_standardisation_process__process__uses_latest_related_records_across_join_sources(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _minimal_source_data(spark, _minimal_hzn_cases_has_df(spark))

        source_data["Horizon_vw_BIS_PlanningAppStrings_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "OLD-APP-REF",
                    "Old Planning",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    1001,
                    "NEW-APP-REF",
                    "New Planning",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("lpaapplicationreference", T.StringType(), True),
                    T.StructField("planningapplicationtype", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseStrings_df"] = spark.createDataFrame(
            [
                (
                    1001,
                    "Old Status",
                    "OLD-LPA",
                    "Old Link",
                    "Old Decision",
                    "Old Jurisdiction",
                    "Y",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    1001,
                    "New Status",
                    "NEW-LPA",
                    "New Link",
                    "New Decision",
                    "New Jurisdiction",
                    "N",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("processingstate", T.StringType(), True),
                    T.StructField("lpacode", T.StringType(), True),
                    T.StructField("linkedstatus", T.StringType(), True),
                    T.StructField("decision", T.StringType(), True),
                    T.StructField("jurisdiction", T.StringType(), True),
                    T.StructField("redetermined", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseInfo_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Old validation", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-001", "New validation", "2025-02-01", "2025-02-01", "2025-02-01", 2),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validity", T.StringType(), True),
                ]
            ),
        )

        source_data["horizon_advert_attributes_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    "OLD-ATTR",
                    "Old Advert",
                    1,
                    "N",
                    "N",
                    "Old Safety",
                    "Old Amenity",
                    "N",
                    "N",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-001",
                    "NEW-ATTR",
                    "New Advert",
                    2,
                    "Y",
                    "Y",
                    "New Safety",
                    "New Amenity",
                    "Y",
                    "Y",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("advertAttributeKey", T.StringType(), True),
                    T.StructField("advertType", T.StringType(), True),
                    T.StructField("setRowNumber", T.IntegerType(), True),
                    T.StructField("publicSafetyLpaIndicator", T.StringType(), True),
                    T.StructField("amenityLPAIndicator", T.StringType(), True),
                    T.StructField("publicSafetyGroundOutcome", T.StringType(), True),
                    T.StructField("amenityGroundOutcome", T.StringType(), True),
                    T.StructField("advertInPosition", T.StringType(), True),
                    T.StructField("siteOnHighwayLand", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_ODW_vw_Inspector_Cases_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    "OLD-INSPECTOR",
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
                (
                    "HAS-001",
                    "NEW-INSPECTOR",
                    "2025-02-01",
                    "2025-02-01",
                    "2025-02-01",
                    2,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("contactid", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["applicationReference"] == "NEW-APP-REF"
        assert row["typeOfPlanningApplication"] == "New Planning"
        assert row["caseStatus"] == "New Status"
        assert row["lpaCode"] == "NEW-LPA"
        assert row["linkedCaseStatus"] == "New Link"
        assert row["caseDecisionOutcome"] == "New Decision"
        assert row["jurisdiction"] == "New Jurisdiction"
        assert row["redeterminedIndicator"] == "N"
        assert row["caseValidationOutcome"] == "New validation"
        assert row["advertAttributeKey"] == "NEW-ATTR"
        assert row["advertType"] == "New Advert"
        assert row["advertSetRowNumber"] == 2
        assert row["inspectorId"] == "NEW-INSPECTOR"

    def test__appeal_has_standardisation_process__process__documents_blank_appellant_procedure_does_not_fallback_to_lpa_like_notebook(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hzn_cases_has_df = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    1001,
                    "HAS",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("caseuniqueid", T.StringType(), True),
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("abbreviation", T.StringType(), True),
                    T.StructField("modifydate", T.StringType(), True),
                ]
            ),
        )

        source_data = _minimal_source_data(spark, hzn_cases_has_df)
        source_data["vw_AddAdditionalData_df"] = spark.createDataFrame(
            [
                (
                    "HAS-001",
                    None,
                    None,
                    "",
                    None,
                    "Written Reps",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-01",
                    1,
                ),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("floorspaceinsquaremetres", T.DoubleType(), True),
                    T.StructField("costsappliedforindicator", T.StringType(), True),
                    T.StructField("procedureappellant", T.StringType(), True),
                    T.StructField("isthesitewithinanaonb", T.StringType(), True),
                    T.StructField("procedurelpa", T.StringType(), True),
                    T.StructField("inspectorneedtoentersite", T.StringType(), True),
                    T.StructField("sitegridreferenceeasting", T.StringType(), True),
                    T.StructField("sitegridreferencenorthing", T.StringType(), True),
                    T.StructField("sitewithinsssi", T.StringType(), True),
                    T.StructField("level", T.StringType(), True),
                ]
            ),
        )

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["lpaProcedurePreference"] == "Written Reps"
        assert row["caseProcedure"] is None
