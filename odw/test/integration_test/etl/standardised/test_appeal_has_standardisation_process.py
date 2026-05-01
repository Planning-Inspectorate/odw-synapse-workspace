import mock
import pytest
import pyspark.sql.types as T
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.standardised.appeal_has_standardisation_process import AppealHasStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

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


def _empty_df(spark, schema):
    return spark.createDataFrame([], schema)


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


class TestRefAppealHasStandardisationProcess(ETLTestCase):
    def test__appeal_has_standardisation_process__run__standardises_end_to_end_and_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hzn_cases_has_df = spark.createDataFrame(
            [
                ("HAS-001", 1001, "HAS", "2025-01-10", "2025-01-10", "2025-01-10", "2025-01-10", 1),
                ("HAS-002", 1002, "HAS", "2025-01-11", "2025-01-11", "2025-01-11", "2025-01-11", 1),
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

        source_data["cases_specialism_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Listed Building", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-001", "Advertisements", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "Enforcement", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("casereference", T.StringType(), True),
                    T.StructField("casespecialism", T.StringType(), True),
                ]
            ),
        )

        source_data["vw_case_dates_df"] = spark.createDataFrame(
            [
                (1001, "2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05", "2025-01-06", "2025-01-07", "2025-01-08", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04", "2025-02-05", "2025-02-06", "2025-02-07", "2025-02-08", "2025-02-01", "2025-02-01", "2025-02-01", 1),
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
                (1001, "2025-03-01", "2025-03-02", "2025-03-03", "2025-03-04", "2025-03-01", "2025-03-01", "2025-03-01", 1),
                (1002, "2025-04-01", "2025-04-02", "2025-04-03", "2025-04-04", "2025-04-01", "2025-04-01", "2025-04-01", 1),
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

        source_data["vw_AddAdditionalData_df"] = spark.createDataFrame(
            [
                ("HAS-001", 50.0, None, "Written Reps", "Y", None, None, "123", "456", "N", "Level 1", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", 75.0, None, None, "N", "Inquiry", None, "789", "012", "Y", "Level 2", "2025-01-01", "2025-01-01", "2025-01-01", 1),
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

        source_data["vw_AdditionalFields_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Important info 1", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "Important info 2", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("importantinformation", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_curr_TypeOfLevel_df"] = spark.createDataFrame(
            [
                ("Level 1", "Band A", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("Level 2", "Band B", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("name", T.StringType(), True),
                    T.StructField("band", T.StringType(), True),
                ]
            ),
        )

        source_data["CaseSiteStrings_df"] = spark.createDataFrame(
            [
                (1001, "Y", "Line 1", "Line 2", "Town 1", "County 1", "AA1 1AA", "UK", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "N", "Line 3", "Line 4", "Town 2", "County 2", "BB1 1BB", "UK", "2025-01-01", "2025-01-01", "2025-01-01", 1),
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

        source_data["Horizon_vw_BIS_PlanningAppStrings_df"] = spark.createDataFrame(
            [
                (1001, "APP-REF-001", "Full Planning", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "APP-REF-002", "Householder", "2025-01-01", "2025-01-01", "2025-01-01", 1),
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
            [
                (1001, "2025-05-01", "2025-05-02", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "2025-06-01", "2025-06-02", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
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
                (1001, "Valid", "LPA01", "Linked", "Allowed", "HAS", "N", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "Started", "LPA02", "Not linked", "Dismissed", "HAS", "Y", "2025-01-01", "2025-01-01", "2025-01-01", 1),
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

        source_data["Horizon_vw_BIS_LeadCase_df"] = spark.createDataFrame(
            [
                (1001, "LEAD-001", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                (1002, "LEAD-002", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("casenodeid", T.IntegerType(), True),
                    T.StructField("leadcasenodeid", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseInfo_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Valid appeal", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "Invalid appeal", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validity", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_CaseDates_df"] = spark.createDataFrame(
            [
                ("HAS-001", "2025-07-01", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "2025-07-02", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("validitystatusdate", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_SpecialistCaseDates_df"] = spark.createDataFrame(
            [
                ("HAS-001", "2025-08-01", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "2025-08-02", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("datecostsreportdespatched", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_vw_BIS_AppealsAdditionalData_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Development 1", None, None, None, None, None, None, None, 1.5, "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "Development 2", None, None, None, None, None, None, None, 2.0, "2025-01-01", "2025-01-01", "2025-01-01", 1),
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

        source_data["Horizon_vw_BIS_CaseSiteCategoryAdditionalStr_df"] = spark.createDataFrame(
            [
                ("HAS-001", "Y", "N", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "N", "Y", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("AppealRefNumber", T.StringType(), True),
                    T.StructField("SiteWithinAGreenBelt", T.StringType(), True),
                    T.StructField("InCARelatesToCA", T.StringType(), True),
                ]
            ),
        )

        source_data["Horizon_ODW_vw_Inspector_Cases_df"] = spark.createDataFrame(
            [
                ("HAS-001", "INSPECTOR-001", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "INSPECTOR-002", "2025-01-01", "2025-01-01", "2025-01-01", 1),
            ],
            _metadata_schema(
                [
                    T.StructField("appealrefnumber", T.StringType(), True),
                    T.StructField("contactid", T.StringType(), True),
                ]
            ),
        )

        source_data["horizon_advert_attributes_df"] = spark.createDataFrame(
            [
                ("HAS-001", "ATTR-001", "Poster", 1, "Y", "N", "Safety 1", "Amenity 1", "Y", "N", "2025-01-01", "2025-01-01", "2025-01-01", 1),
                ("HAS-002", "ATTR-002", "Display", 2, "N", "Y", "Safety 2", "Amenity 2", "N", "Y", "2025-01-01", "2025-01-01", "2025-01-01", 1),
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

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.standardised.appeal_has_standardisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealHasStandardisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

        rows = {row["caseReference"]: row for row in actual_df.orderBy("caseReference").collect()}

        assert actual_df.count() == 2
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 2

        assert rows["HAS-001"]["caseId"] == 1001
        assert rows["HAS-001"]["caseType"] == "HAS"
        assert rows["HAS-001"]["caseProcedure"] == "WR"
        assert rows["HAS-001"]["allocationLevel"] == "Level 1"
        assert rows["HAS-001"]["allocationBand"] == "Band A"
        assert rows["HAS-001"]["caseSpecialisms"] == "Advertisements, Listed Building"
        assert rows["HAS-001"]["siteAddressLine1"] == "Line 1"
        assert rows["HAS-001"]["siteAddressTown"] == "Town 1"
        assert rows["HAS-001"]["siteAreaSquareMetres"] == 15000.0
        assert rows["HAS-001"]["floorSpaceSquareMetres"] == 50.0
        assert rows["HAS-001"]["originalDevelopmentDescription"] == "Development 1"
        assert rows["HAS-001"]["caseStatus"] == "Valid"
        assert rows["HAS-001"]["lpaCode"] == "LPA01"
        assert rows["HAS-001"]["leadCaseReference"] == "LEAD-001"
        assert rows["HAS-001"]["caseValidationOutcome"] == "Valid appeal"
        assert rows["HAS-001"]["caseValidationDate"] == "2025-07-01"
        assert rows["HAS-001"]["dateCostsReportDespatched"] == "2025-08-01"
        assert rows["HAS-001"]["isGreenBelt"] == "Y"
        assert rows["HAS-001"]["inConservationArea"] == "N"
        assert rows["HAS-001"]["inspectorId"] == "INSPECTOR-001"
        assert rows["HAS-001"]["importantinformation"] == "Important info 1"
        assert rows["HAS-001"]["advertAttributeKey"] == "ATTR-001"

        assert rows["HAS-002"]["caseId"] == 1002
        assert rows["HAS-002"]["caseProcedure"] == "LI"
        assert rows["HAS-002"]["allocationLevel"] == "Level 2"
        assert rows["HAS-002"]["allocationBand"] == "Band B"
        assert rows["HAS-002"]["caseSpecialisms"] == "Enforcement"
        assert rows["HAS-002"]["siteAddressPostcode"] == "BB1 1BB"
        assert rows["HAS-002"]["siteAreaSquareMetres"] == 20000.0
        assert rows["HAS-002"]["caseStatus"] == "Started"
        assert rows["HAS-002"]["lpaCode"] == "LPA02"
        assert rows["HAS-002"]["leadCaseReference"] == "LEAD-002"
        assert rows["HAS-002"]["caseValidationOutcome"] == "Invalid appeal"
        assert rows["HAS-002"]["isGreenBelt"] == "N"
        assert rows["HAS-002"]["inConservationArea"] == "Y"
        assert rows["HAS-002"]["inspectorId"] == "INSPECTOR-002"
        assert rows["HAS-002"]["advertAttributeKey"] == "ATTR-002"