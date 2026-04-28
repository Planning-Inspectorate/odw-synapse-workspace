import mock
import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType, IntegerType
from odw.core.etl.transformation.standardised.appeal_s78_standardisation_process import AppealS78StandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
from odw.test.util.assertion import assert_dataframes_equal
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from typing import Callable


class TestAppealS78StandardisationProcess(SparkTestCase):
    @pytest.fixture()
    def mock_data_loaders(self):
        yield

    def _assert_row_count_query(self, target_col_name: str, override_property: str, test_name: str, target_function: Callable):
        """
        Tests for the data loaders that execute a query with the below structure

        ```
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY target_col_name
                    ORDER BY expected_from DESC, modified_datetime DESC, ingested_datetime DESC, file_id DESC
                ) rn
            FROM TABLE_NAME
        ) WHERE rn = 1
        ```
        :param str target_col_name: The name of the partitioning column
        :param str override_property: Static variable in AppealS78StandardisationProcess which defines the table to query
        :param str test_name: The name of the test
        :param Callable target_function: The function of AppealS78StandardisationProcess which loads the data (i.e. the function to test)
        """
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "modified_datetime": None,
                "file_id": None,
                target_col_name: None,
            }
            return base | overrides

        def generate_expected_row(**overrides):
            base = {"ingested_datetime": None, "expected_from": None, "modified_datetime": None, "file_id": None, target_col_name: None, "rn": 1}
            return base | overrides

        expected_from_base = datetime(2025, 1, 1)
        modified_datetime_base = datetime(2025, 1, 1)
        ingested_datetime_base = datetime(2025, 1, 1)
        file_id_base = "a"

        expected_from_inc = expected_from_base + timedelta(days=1)
        modified_datetime_inc = modified_datetime_base + timedelta(days=1)
        ingested_datetime_inc = ingested_datetime_base + timedelta(days=1)
        file_id_inc = "b"

        raw_data = spark.createDataFrame(
            (
                generate_row(**{target_col_name: "1"}),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_row(
                    **{target_col_name: "1"},
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_row(**{target_col_name: "2"}),
                generate_row(**{target_col_name: "3"}),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField(target_col_name, StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )
        self.write_existing_table(
            spark,
            raw_data,
            test_name,
            "odw_standardised_db",
            "odw-standardised",
            test_name,
            "overwrite",
        )
        expected_data = spark.createDataFrame(
            (
                generate_expected_row(
                    **{target_col_name: "1"},
                    ingested_datetime=ingested_datetime_inc,
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    rn=1,
                ),
                generate_expected_row(**{target_col_name: "2"}, ingested_datetime=None, expected_from=None, modified_datetime=None),
                generate_expected_row(**{target_col_name: "3"}, ingested_datetime=None, expected_from=None, modified_datetime=None),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField(target_col_name, StringType(), True),
                    StructField("file_id", StringType(), True),
                    StructField("rn", IntegerType(), False),
                ]
            ),
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, override_property, test_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            inst = AppealS78StandardisationProcess()
            actual_data = target_function(inst)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__load_horizoncases_s78(self):
        self._assert_row_count_query(
            "caseuniqueid", "STANDARDISED_HORIZON_CASES_S78", "t_as78sp_lhcs78", AppealS78StandardisationProcess._load_standardised_horizoncases_s78
        )

    def test__appeal_s78_standardisation_process__load_cases_specialisms(self):
        override_table_name = "t_as78sp_lcs"

        def generate_case_specialisms_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casereference": "",
                "lastmodified": None,
                "casespecialism": None,
                "input_file": "somefile.parquet",
                "ingested_by_process_name": "some_process",
                "modified_datetime": None,
                "modified_by_process_name": "some_process",
                "entity_name": "cases_specialisms",
                "file_id": None,
            }
            return base | overrides

        spark = PytestSparkSessionUtil().get_spark_session()
        raw_data = spark.createDataFrame(
            (
                generate_case_specialisms_row(file_id="1", casereference="refA"),
                generate_case_specialisms_row(file_id="2", casereference="refB", casespecialism=""),
                generate_case_specialisms_row(file_id="2", casereference="refC", casespecialism="Some Specialism"),
                generate_case_specialisms_row(file_id="3", casereference="refD", casespecialism="Some Specialism"),
                generate_case_specialisms_row(file_id="4", casereference="refE", casespecialism="Another Specialism"),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casereference", StringType(), True),
                    StructField("lastmodified", StringType(), True),
                    StructField("casespecialism", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )
        self.write_existing_table(
            spark,
            raw_data,
            override_table_name,
            "odw_standardised_db",
            "odw-standardised",
            override_table_name,
            "overwrite",
        )
        # Generated by running the original query against the test data
        expected_data = spark.createDataFrame(
            (
                {"casereference": "refC", "casespecialism": "Some Specialism"},
                {"casereference": "refD", "casespecialism": "Some Specialism"},
                {"casereference": "refE", "casespecialism": "Another Specialism"},
            ),
            schema=StructType([StructField("casereference", StringType(), True), StructField("casespecialism", StringType(), False)]),
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_CASES_SPECIALISMS", override_table_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_cases_specialisms()
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__load_vw_case_dates(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_HORIZON_CASE_DATES", "t_as78sp_lvwcd", AppealS78StandardisationProcess._load_standardised_vw_case_dates
        )

    def test__appeal_s78_standardisation_process__load_casedocumentdatesdates(self):
        self._assert_row_count_query(
            "casenodeid",
            "STANDARDISED_CASE_DOCUMENT_DATES_DATES",
            "t_as78sp_lcdd",
            AppealS78StandardisationProcess._load_standardised_casedocumentdatesdates,
        )

    def test__appeal_s78_standardisation_process__load_casesitestrings(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_CASE_SITE_STRINGS", "t_as78sp_lcss", AppealS78StandardisationProcess._load_standardised_casesitestrings
        )

    def test__appeal_s78_standardisation_process__load_type_of_procedure(self):
        # The full source table should be returned
        test_name = "t_as78sp_ltop"
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_row(**overrides):
            base = {"colA": "1", "colB": "a", "colC": "d"}
            return base | overrides

        raw_data = spark.createDataFrame(
            (
                generate_row(colA=1),
                generate_row(colB=2),
                generate_row(colC=3),
            ),
            schema=StructType(
                [StructField("colA", StringType(), True), StructField("colB", StringType(), True), StructField("colC", StringType(), True)]
            ),
        )
        self.write_existing_table(
            spark,
            raw_data,
            test_name,
            "odw_standardised_db",
            "odw-standardised",
            test_name,
            "overwrite",
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_TYPE_OF_PROCEDURE", test_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_horizon_appeal_grounds()
            assert_dataframes_equal(raw_data, actual_data)

    def test__appeal_s78_standardisation_process__load_vw_addadditionaldata(self):
        self._assert_row_count_query(
            "appealrefnumber",
            "STANDARDISED_ADD_ADDITIONAL_DATA",
            "t_as78sp_lvwaad",
            AppealS78StandardisationProcess._load_standardised_vw_additionalfields,
        )

    def test__appeal_s78_standardisation_process__load_vw_additionalfields(self):
        self._assert_row_count_query(
            "appealrefnumber", "STANDARDISED_ADDITIONAL_FIELDSt_as78sp_lvwaf", AppealS78StandardisationProcess._load_standardised_vw_additionalfields
        )

    def test__appeal_s78_standardisation_process__load_horizon_advert_attributes(self):
        self._assert_row_count_query(
            "caseuniqueid",
            "STANDARDISED_HORIZON_ADVERT_ATTRIBUTES",
            "t_as78sp_lhaa",
            AppealS78StandardisationProcess._load_standardised_horizon_advert_attributes,
        )

    def test__appeal_s78_standardisation_process__load_current_type_of_level(self):
        test_name = "t_as78sp_lctol"
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_row(**overrides):
            base = {"colA": "1", "colB": "a", "colC": "d"}
            return base | overrides

        raw_data = spark.createDataFrame(
            (
                generate_row(colA=1),
                generate_row(colB=2),
                generate_row(colC=3),
            ),
            schema=StructType(
                [StructField("colA", StringType(), True), StructField("colB", StringType(), True), StructField("colC", StringType(), True)]
            ),
        )
        self.write_existing_table(
            spark,
            raw_data,
            test_name,
            "odw_standardised_db",
            "odw-standardised",
            test_name,
            "overwrite",
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_TYPE_OF_LEVEL", test_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_current_type_of_level()
            assert_dataframes_equal(raw_data, actual_data)

    def test__appeal_s78_standardisation_process__load_horizon_specialist_case_dates(self):
        self._assert_row_count_query(
            "appealrefnumber",
            "STANDARDISED_HORIZON_SPECIALIST_CASE_DATES",
            "t_as78sp_lhscd",
            AppealS78StandardisationProcess._load_standardised_horizon_specialist_case_dates,
        )

    def test__appeal_s78_standardisation_process__load_PlanningAppStrings(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_PLANNING_APP_STRINGS", "t_as78sp_lpas", AppealS78StandardisationProcess._load_standardised_PlanningAppStrings
        )

    def test__appeal_s78_standardisation_process__load_PlanningAppDates(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_PLANNING_APP_DATES", "t_as78sp_lpad", AppealS78StandardisationProcess._load_standardised_PlanningAppDates
        )

    def test__appeal_s78_standardisation_process__load_BIS_LeadCase(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_LEAD_CASE", "t_as78sp_lblc", AppealS78StandardisationProcess._load_standardised_BIS_LeadCase
        )

    def test__appeal_s78_standardisation_process__load_CaseStrings(self):
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_CASE_STRINGS", "t_as78sp_lcs", AppealS78StandardisationProcess._load_standardised_CaseStrings
        )

    def test__appeal_s78_standardisation_process__load_horizon_case_info(self):
        self._assert_row_count_query(
            "appealrefnumber", "STANDARDISED_HORIZON_CASE_INFO", "t_as78sp_lhci", AppealS78StandardisationProcess._load_standardised_horizon_case_info
        )

    def test__appeal_s78_standardisation_process__load_horizon_case_dates(self):
        self._assert_row_count_query(
            "appealrefnumber",
            "STANDARDISED_HORIZON_CASE_DATES",
            "t_as78sp_lhcd",
            AppealS78StandardisationProcess._load_standardised_horizon_case_dates,
        )

    def test__appeal_s78_standardisation_process__load_horizon_appeals_additional_data(self):
        self._assert_row_count_query(
            "appealrefnumber",
            "STANDARDISED_HORIZON_APPEALS_ADDITIONAL_DATA",
            "t_as78sp_lhaad",
            AppealS78StandardisationProcess._load_standardised_horizon_appeals_additional_data,
        )

    def test__appeal_s78_standardisation_process__load_horizon_appeal_grounds(self):
        test_name = "t_as78sp_lhap"
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_row(**overrides):
            base = {"casenodeid": None, "appealgroundletter": None, "groundforappealstartdate": None}
            return base | overrides

        def generate_expected_row(**overrides):
            base = {"casenodeid": None, "enforcementAppealGroundsDetails": [{"appealGroundLetter": "", "groundForAppealStartDate": ""}]}
            return base | overrides

        base_date = datetime(2025, 1, 1)
        next_date = datetime(2025, 2, 1)

        raw_data = spark.createDataFrame(
            (
                generate_row(
                    casenodeid=1,
                    appealgroundletter="a",
                    groundforappealstartdate=base_date,
                ),
                generate_row(
                    casenodeid=1,
                    appealgroundletter="a",
                    groundforappealstartdate=next_date,
                ),
                generate_row(
                    casenodeid=1,
                    appealgroundletter="b",
                    groundforappealstartdate=base_date,
                ),
                generate_row(
                    casenodeid=2,
                    appealgroundletter="a",
                    groundforappealstartdate=base_date,
                ),
                generate_row(
                    casenodeid=2,
                    appealgroundletter="a",
                    groundforappealstartdate=next_date,
                ),
                generate_row(
                    casenodeid=2,
                    appealgroundletter="b",
                    groundforappealstartdate=base_date,
                ),
                generate_row(
                    casenodeid=2,
                    appealgroundletter="b",
                    groundforappealstartdate=next_date,
                ),
            ),
            schema=StructType(
                [
                    StructField("casenodeid", IntegerType(), True),
                    StructField("appealgroundletter", StringType(), True),
                    StructField("groundforappealstartdate", TimestampType(), True),
                ]
            ),
        )
        self.write_existing_table(
            spark,
            raw_data,
            test_name,
            "odw_standardised_db",
            "odw-standardised",
            test_name,
            "overwrite",
        )

        expected_data = spark.createDataFrame(
            (
                generate_expected_row(
                    casenodeid=1,
                    enforcementAppealGroundsDetails=[
                        {"appealGroundLetter": "a", "groundForAppealStartDate": base_date},
                        {"appealGroundLetter": "a", "groundForAppealStartDate": next_date},
                        {"appealGroundLetter": "b", "groundForAppealStartDate": base_date},
                    ],
                ),
                generate_expected_row(
                    casenodeid=2,
                    enforcementAppealGroundsDetails=[
                        {"appealGroundLetter": "a", "groundForAppealStartDate": base_date},
                        {"appealGroundLetter": "a", "groundForAppealStartDate": next_date},
                        {"appealGroundLetter": "b", "groundForAppealStartDate": base_date},
                        {"appealGroundLetter": "b", "groundForAppealStartDate": next_date},
                    ],
                ),
            ),
            schema=StructType(
                [
                    StructField("casenodeid", IntegerType(), True),
                    StructField(
                        "enforcementAppealGroundsDetails",
                        ArrayType(
                            StructType(
                                [
                                    StructField("appealGroundLetter", StringType(), True),
                                    StructField("groundForAppealStartDate", TimestampType(), True),
                                ]
                            ),
                            False,
                        ),
                        False,
                    ),
                ]
            ),
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_HORIZON_APPEAL_GROUNDS", test_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_horizon_appeal_grounds()
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__load_horizon_notice_dates(self):
        self._assert_row_count_query(
            "casenodeid",
            "STANDARDISED_HORIZON_NOTICE_DATES",
            "t_as78sp_lhnd",
            AppealS78StandardisationProcess._load_standardised_horizon_notice_dates,
        )

    def test__appeal_s78_standardisation_process__load_horizon_application_made_under_section(self):
        self._assert_row_count_query(
            "casenodeid",
            "STANDARDISED_HORIZON_APPLICATION_MADE_UNDER_SECTION",
            "t_as78sp_lhmu",
            AppealS78StandardisationProcess._load_standardised_horizon_application_made_under_section,
        )

    def test__appeal_s78_standardisation_process__load_data(self, mock_data_loaders):
        expected_output_keys = {
            "odw_standardised_db.horizoncases_s78",
            "odw_standardised_db.cases_specialisms",
            "odw_standardised_db.vw_case_dates",
            "odw_standardised_db.casedocumentdatesdates",
            "odw_standardised_db.casesitestrings",
            "odw_standardised_db.typeofprocedure",
            "odw_standardised_db.vw_addadditionaldata",
            "odw_standardised_db.vw_additionalfields",
            "odw_standardised_db.horizon_advert_attributes",
            "odw_standardised_db.TypeOfLevel",
            "odw_standardised_db.horizon_specialist_case_dates",
            "odw_standardised_db.PlanningAppStrings",
            "odw_standardised_db.PlanningAppDates",
            "odw_standardised_db.BIS_LeadCase",
            "odw_standardised_db.CaseStrings",
            "odw_standardised_db.horizon_case_info",
            "odw_standardised_db.horizon_case_dates",
            "odw_standardised_db.horizon_appeals_additional_data",
            "odw_standardised_db.horizon_appeal_grounds",
            "odw_standardised_db.horizon_notice_dates",
            "odw_standardised_db.horizon_application_made_under_section",
        }
