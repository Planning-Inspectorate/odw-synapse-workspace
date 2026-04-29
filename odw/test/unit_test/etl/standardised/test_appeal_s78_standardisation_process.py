import mock
import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType, IntegerType, BooleanType
from odw.core.etl.transformation.standardised.appeal_s78_standardisation_process import AppealS78StandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
from odw.test.util.assertion import assert_dataframes_equal
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from typing import Callable
from contextlib import ExitStack


class TestAppealS78StandardisationProcess(SparkTestCase):
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
        # h_1row
        self._assert_row_count_query(
            "caseuniqueid", "STANDARDISED_HORIZON_CASES_S78", "t_as78sp_lhcs78", AppealS78StandardisationProcess._load_standardised_horizoncases_s78
        )

    def test__appeal_s78_standardisation_process__load_cases_specialisms(self):
        # cs_agg
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
        # cd_1row
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_HORIZON_CASE_DATES", "t_as78sp_lvwcd", AppealS78StandardisationProcess._load_standardised_vw_case_dates
        )

    def test__appeal_s78_standardisation_process__load_casedocumentdatesdates(self):
        # cdd_1row
        self._assert_row_count_query(
            "casenodeid",
            "STANDARDISED_CASE_DOCUMENT_DATES_DATES",
            "t_as78sp_lcdd",
            AppealS78StandardisationProcess._load_standardised_casedocumentdatesdates,
        )

    def test__appeal_s78_standardisation_process__load_casesitestrings(self):
        # css_1row
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
            AppealS78StandardisationProcess._load_standardised_vw_addadditionaldata,
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

    def test__appeal_s78_standardisation_process__load_planning_app_strings(self):
        # pas_1row
        # For odw_standardised_db.PlanningAppStrings
        self._assert_row_count_query(
            "casenodeid",
            "STANDARDISED_PLANNING_APP_STRINGS",
            "t_as78sp_lpas",
            AppealS78StandardisationProcess._load_standardised_planning_app_strings,
        )

    def test__appeal_s78_standardisation_process__load_planning_app_dates(self):
        # For odw_standardised_db.PlanningAppDates
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_PLANNING_APP_DATES", "t_as78sp_lpad", AppealS78StandardisationProcess._load_planning_app_dates
        )

    def test__appeal_s78_standardisation_process__load_standardised_bis_lead_case(self):
        # For odw_standardised_db.BIS_LeadCase
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_LEAD_CASE", "t_as78sp_lblc", AppealS78StandardisationProcess._load_standardised_bis_lead_case
        )

    def test__appeal_s78_standardisation_process__load_standardised_case_strings(self):
        # For odw_standardised_db.CaseStrings
        self._assert_row_count_query(
            "casenodeid", "STANDARDISED_CASE_STRINGS", "t_as78sp_lcs", AppealS78StandardisationProcess._load_standardised_case_strings
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

    def test__appeal_s78_standardisation_process__load_data(self):
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            inst = AppealS78StandardisationProcess()
            loader_function_map = {
                "odw_standardised_db.horizoncases_s78": inst._load_standardised_horizoncases_s78,
                "odw_standardised_db.cases_specialisms": inst._load_standardised_cases_specialisms,
                "odw_standardised_db.vw_case_dates": inst._load_standardised_vw_case_dates,
                "odw_standardised_db.casedocumentdatesdates": inst._load_standardised_casedocumentdatesdates,
                "odw_standardised_db.casesitestrings": inst._load_standardised_casesitestrings,
                "odw_standardised_db.typeofprocedure": inst._load_type_of_procedure,
                "odw_standardised_db.vw_addadditionaldata": inst._load_standardised_vw_addadditionaldata,
                "odw_standardised_db.vw_additionalfields": inst._load_standardised_vw_additionalfields,
                "odw_standardised_db.horizon_advert_attributes": inst._load_standardised_horizon_advert_attributes,
                "odw_standardised_db.TypeOfLevel": inst._load_standardised_current_type_of_level,
                "odw_standardised_db.horizon_specialist_case_dates": inst._load_standardised_horizon_specialist_case_dates,
                "odw_standardised_db.PlanningAppStrings": inst._load_standardised_planning_app_strings,
                "odw_standardised_db.PlanningAppDates": inst._load_planning_app_dates,
                "odw_standardised_db.BIS_LeadCase": inst._load_standardised_bis_lead_case,
                "odw_standardised_db.CaseStrings": inst._load_standardised_case_strings,
                "odw_standardised_db.horizon_case_info": inst._load_standardised_horizon_case_info,
                "odw_standardised_db.horizon_case_dates": inst._load_standardised_horizon_case_dates,
                "odw_standardised_db.horizon_appeals_additional_data": inst._load_standardised_horizon_appeals_additional_data,
                "odw_standardised_db.horizon_appeal_grounds": inst._load_standardised_horizon_appeal_grounds,
                "odw_standardised_db.horizon_notice_dates": inst._load_standardised_horizon_notice_dates,
                "odw_standardised_db.horizon_application_made_under_section": inst._load_standardised_horizon_application_made_under_section,
            }
            expected_result = {table_name: i for i, table_name in enumerate(loader_function_map.keys())}
            with ExitStack() as stack:
                for table_name, return_value in expected_result.items():
                    method = loader_function_map[table_name].__name__
                    stack.enter_context(mock.patch.object(inst, method, return_value=return_value))
                actual_result = inst.load_data()
                called_functions_map = {x.__name__: getattr(inst, x.__name__).called for x in loader_function_map.values()}
                uncalled_functions = [k for k, v in called_functions_map.items() if not v]
                assert not uncalled_functions, (
                    f"The following methods of AppealS78StandardisationProcess were not called by load_data() but were expected {uncalled_functions}"
                )
                assert expected_result == actual_result

    def test__appeal_s78_standardisation_process__generate_base_table(self):
        test_name = "t_as78sp_gbt"
        spark = PytestSparkSessionUtil().get_spark_session()

        # h_1row
        def generate_horizon_case_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": None,
                "subtype": None,
                "modifydate": None,
                "abbreviation": None,
                "casereference": None,
                "caseuniqueid": None,
                "caseofficerid": None,
                "caseofficerlogin": None,
                "caseofficername": None,
                "coemailaddress": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        horizon_cases = spark.createDataFrame(
            (
                generate_horizon_case_row(casenodeid="1", caseuniqueid="refA"),
                generate_horizon_case_row(casenodeid="2", caseuniqueid="refB"),
                generate_horizon_case_row(casenodeid="3", caseuniqueid="refC"),
                generate_horizon_case_row(casenodeid="4", caseuniqueid="refD"),
                generate_horizon_case_row(casenodeid="5", caseuniqueid="refE"),
                generate_horizon_case_row(casenodeid="6", caseuniqueid="refF"),
                generate_horizon_case_row(casenodeid="7", caseuniqueid="refG"),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("subtype", StringType(), True),
                    StructField("modifydate", StringType(), True),
                    StructField("abbreviation", StringType(), True),
                    StructField("casereference", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("caseofficerid", StringType(), True),
                    StructField("caseofficerlogin", StringType(), True),
                    StructField("caseofficername", StringType(), True),
                    StructField("coemailaddress", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )
        # cs_agg
        case_specialisms = spark.createDataFrame(
            ({"casereference": "refA", "casespecialism": "Some Specialism"},),
            schema=StructType([StructField("casereference", StringType(), True), StructField("casespecialism", StringType(), False)]),
        )

        # cd_1row
        def generate_case_dates_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": None,
                "receiptdate": None,
                "startdate": None,
                "appealdocscomplete": None,
                "callindate": None,
                "datereceivedfromcallinauthority": None,
                "bespoketargetdate": None,
                "daterecovered": None,
                "datenotrecoveredorderecovered": None,
                "decisionsubmitdate": None,
                "noticewithdrawndate": None,
                "appealwithdrawndate": None,
                "casedecisiondate": None,
                "appealturnedawaydate": None,
                "appeallapseddate": None,
                "casecloseddate": None,
                "proceduredetermineddate": None,
                "originalcasedecisiondate": None,
                "planningguaranteedate": None,
                "datedecisionreportreceivedinpins": None,
                "datesenttoreader": None,
                "datereturnedfromreader": None,
                "datepublicationprocedurecompleted": None,
                "datecostsreportdespatched": None,
                "orderrejectedorreturned": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        case_dates = spark.createDataFrame(
            (
                generate_case_dates_row(
                    casenodeid="2",
                    receiptdate=datetime(2025, 1, 1).isoformat(),
                    appealdocscomplete="Romulan",
                    startdate=datetime(2026, 1, 1).isoformat(),
                    appealwithdrawndate=datetime(2027, 1, 1).isoformat(),
                    casedecisiondate=datetime(2028, 1, 1).isoformat(),
                    datenotrecoveredorderecovered=datetime(2029, 1, 1).isoformat(),
                    daterecovered=datetime(2030, 1, 1).isoformat(),
                    originalcasedecisiondate=datetime(2031, 1, 1).isoformat(),
                ),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("receiptdate", StringType(), True),
                    StructField("startdate", StringType(), True),
                    StructField("appealdocscomplete", StringType(), True),
                    StructField("callindate", StringType(), True),
                    StructField("datereceivedfromcallinauthority", StringType(), True),
                    StructField("bespoketargetdate", StringType(), True),
                    StructField("daterecovered", StringType(), True),
                    StructField("datenotrecoveredorderecovered", StringType(), True),
                    StructField("decisionsubmitdate", StringType(), True),
                    StructField("noticewithdrawndate", StringType(), True),
                    StructField("appealwithdrawndate", StringType(), True),
                    StructField("casedecisiondate", StringType(), True),
                    StructField("appealturnedawaydate", StringType(), True),
                    StructField("appeallapseddate", StringType(), True),
                    StructField("casecloseddate", StringType(), True),
                    StructField("proceduredetermineddate", StringType(), True),
                    StructField("originalcasedecisiondate", StringType(), True),
                    StructField("planningguaranteedate", StringType(), True),
                    StructField("datedecisionreportreceivedinpins", StringType(), True),
                    StructField("datesenttoreader", StringType(), True),
                    StructField("datereturnedfromreader", StringType(), True),
                    StructField("datepublicationprocedurecompleted", StringType(), True),
                    StructField("datecostsreportdespatched", StringType(), True),
                    StructField("orderrejectedorreturned", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # cdd_1row
        def generate_case_document_dates_dates_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": None,
                "appealrefnumber": None,
                "questionnairedue": None,
                "questionnairereceived": None,
                "lpaconditionssubmitted": None,
                "lpaconditionsforwarded": None,
                "statementduedate": None,
                "appellantstatementsubmitted": None,
                "appellantstatementforwarded": None,
                "lpastatementsubmitted": None,
                "lpastatementforwarded": None,
                "datenotificationlettersent": None,
                "interestedpartyrepsduedate": None,
                "interestedpartyrepsforwarded": None,
                "finalcommentsdue": None,
                "appellantcommentssubmitted": None,
                "appellantcommentsforwarded": None,
                "lpacommentssubmitted": None,
                "lpacommentsforwarded": None,
                "commongrounddue": None,
                "commongroundreceived": None,
                "sitenoticesent": None,
                "proofsdue": None,
                "appellantsproofssubmitted": None,
                "appellantsproofsforwarded": None,
                "lpaproofssubmitted": None,
                "lpaproofsforwarded": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        case_document_dates_dates = spark.createDataFrame(
            (
                generate_case_document_dates_dates_row(
                    casenodeid="3",
                    questionnairedue="Klingon",
                    questionnairereceived="Cardassian",
                    interestedpartyrepsduedate=datetime(2032, 1, 1).isoformat(),
                    proofsdue="Vulcan",
                ),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("appealrefnumber", StringType(), True),
                    StructField("questionnairedue", StringType(), True),
                    StructField("questionnairereceived", StringType(), True),
                    StructField("lpaconditionssubmitted", StringType(), True),
                    StructField("lpaconditionsforwarded", StringType(), True),
                    StructField("statementduedate", StringType(), True),
                    StructField("appellantstatementsubmitted", StringType(), True),
                    StructField("appellantstatementforwarded", StringType(), True),
                    StructField("lpastatementsubmitted", StringType(), True),
                    StructField("lpastatementforwarded", StringType(), True),
                    StructField("datenotificationlettersent", StringType(), True),
                    StructField("interestedpartyrepsduedate", StringType(), True),
                    StructField("interestedpartyrepsforwarded", StringType(), True),
                    StructField("finalcommentsdue", StringType(), True),
                    StructField("appellantcommentssubmitted", StringType(), True),
                    StructField("appellantcommentsforwarded", StringType(), True),
                    StructField("lpacommentssubmitted", StringType(), True),
                    StructField("lpacommentsforwarded", StringType(), True),
                    StructField("commongrounddue", StringType(), True),
                    StructField("commongroundreceived", StringType(), True),
                    StructField("sitenoticesent", StringType(), True),
                    StructField("proofsdue", StringType(), True),
                    StructField("appellantsproofssubmitted", StringType(), True),
                    StructField("appellantsproofsforwarded", StringType(), True),
                    StructField("lpaproofssubmitted", StringType(), True),
                    StructField("lpaproofsforwarded", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # css_1row
        def generate_case_site_strings_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": None,
                "landuse": None,
                "areaofsite": None,
                "addressline1": None,
                "addressline2": None,
                "town": None,
                "county": None,
                "postcode": None,
                "country": None,
                "greenbelt": None,
                "siteviewablefromroad": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        case_site_strings = spark.createDataFrame(
            (generate_case_site_strings_row(casenodeid="4", siteviewablefromroad="Kelpian"),),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("landuse", StringType(), True),
                    StructField("areaofsite", StringType(), True),
                    StructField("addressline1", StringType(), True),
                    StructField("addressline2", StringType(), True),
                    StructField("town", StringType(), True),
                    StructField("county", StringType(), True),
                    StructField("postcode", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("greenbelt", StringType(), True),
                    StructField("siteviewablefromroad", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # aad_old_1row
        def generate_addadditional_data_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "appealrefnumber": None,
                "caseprocess": None,
                "caseworkmarker": None,
                "costsappliedforindicator": None,
                "level": None,
                "procedureappellant": None,
                "procedurelpa": None,
                "proceduredetermineddate": None,
                "targetdate": None,
                "agriculturalholding": None,
                "developmentaffectsettingoflistedbuilding": None,
                "floorspaceinsquaremetres": None,
                "sitegridreferenceeasting": None,
                "sitegridreferencenorthing": None,
                "historicbuildinggrantmade": None,
                "incarelatestoca": None,
                "inspectorneedtoentersite": None,
                "isfloodinganissue": None,
                "isthesitewithinanaonb": None,
                "sitewithinsssi": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        addadditional_data = spark.createDataFrame(
            (
                generate_addadditional_data_row(
                    appealrefnumber="refB",
                    floorspaceinsquaremetres="5",
                    costsappliedforindicator="10",
                    procedureappellant="Bob Vance",
                    isthesitewithinanaonb="No",
                    procedurelpa="Some LPA",
                    inspectorneedtoentersite="Perchance",
                    sitegridreferenceeasting="Some easting",
                    sitegridreferencenorthing="Some northing",
                    sitewithinsssi="Some SSSI",
                    level="Some level",
                ),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("appealrefnumber", StringType(), True),
                    StructField("caseprocess", StringType(), True),
                    StructField("caseworkmarker", StringType(), True),
                    StructField("costsappliedforindicator", StringType(), True),
                    StructField("level", StringType(), True),
                    StructField("procedureappellant", StringType(), True),
                    StructField("procedurelpa", StringType(), True),
                    StructField("proceduredetermineddate", StringType(), True),
                    StructField("targetdate", StringType(), True),
                    StructField("agriculturalholding", StringType(), True),
                    StructField("developmentaffectsettingoflistedbuilding", StringType(), True),
                    StructField("floorspaceinsquaremetres", StringType(), True),
                    StructField("sitegridreferenceeasting", StringType(), True),
                    StructField("sitegridreferencenorthing", StringType(), True),
                    StructField("historicbuildinggrantmade", StringType(), True),
                    StructField("incarelatestoca", StringType(), True),
                    StructField("inspectorneedtoentersite", StringType(), True),
                    StructField("isfloodinganissue", StringType(), True),
                    StructField("isthesitewithinanaonb", StringType(), True),
                    StructField("sitewithinsssi", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # af_1row
        def generate_additional_fields_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "appealrefnumber": None,
                "casereference": None,
                "standardpriority": None,
                "importantinformation": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        additional_fields = spark.createDataFrame(
            (generate_additional_fields_row(appealrefnumber="refC", importantinformation="Makarov knows Yuri"),),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("appealrefnumber", StringType(), True),
                    StructField("casereference", StringType(), True),
                    StructField("standardpriority", StringType(), True),
                    StructField("importantinformation", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # hnd_1row
        def generate_notice_dates_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "caseUniqueId": None,
                "caseNodeId": None,
                "setRowNumber": None,
                "issueDate": None,
                "effectiveDate": None,
                "ingested_by_process_name": None,
                "input_file": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
            }
            return base | overrides

        horizon_notice_dates = spark.createDataFrame(
            (generate_notice_dates_row(caseNodeId=5, issueDate=datetime(2033, 1, 1), effectiveDate=datetime(2034, 1, 1)),),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("caseUniqueId", IntegerType(), True),
                    StructField("caseNodeId", IntegerType(), True),
                    StructField("setRowNumber", IntegerType(), True),
                    StructField("issueDate", TimestampType(), True),
                    StructField("effectiveDate", TimestampType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )

        # hag_agg
        def generate_appeal_grounds_row(**overrides):
            base = {"casenodeid": None, "enforcementAppealGroundsDetails": []}
            return base | overrides

        horizon_appeal_grounds = spark.createDataFrame(
            (
                generate_appeal_grounds_row(
                    casenodeid=6, enforcementAppealGroundsDetails=[{"appealGroundLetter": "A", "groundForAppealStartDate": None}]
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

        # Expected data
        def generate_expected_data_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": None,
                "subtype": None,
                "modifydate": None,
                "abbreviation": None,
                "casereference": None,
                "caseuniqueid": None,
                "caseofficerid": None,
                "caseofficerlogin": None,
                "caseofficername": None,
                "coemailaddress": None,
                "input_file": None,
                "ingested_by_process_name": None,
                "modified_datetime": None,
                "modified_by_process_name": None,
                "entity_name": None,
                "file_id": None,
                "casespecialism": None,
                "caseCreatedDate": None,
                "appealdocscomplete": None,
                "startdate": None,
                "appealwithdrawndate": None,
                "casedecisiondate": None,
                "datenotrecoveredorderecovered": None,
                "daterecovered": None,
                "originalcasedecisiondate": None,
                "questionnairedue": None,
                "questionnairereceived": None,
                "interestedpartyrepsduedate": None,
                "proofsdue": None,
                "siteviewablefromroad": None,
                "floorspaceinsquaremetres": None,
                "costsappliedforindicator": None,
                "procedureappellant": None,
                "isthesitewithinanaonb": None,
                "procedurelpa": None,
                "inspectorneedtoentersite": None,
                "sitegridreferenceeasting": None,
                "sitegridreferencenorthing": None,
                "sitewithinsssi": None,
                "importantinformation": None,
                "level_code": None,
                "issueDateOfEnforcementNotice": None,
                "effectiveDateOfEnforcementNotice": None,
                "enforcementAppealGroundsDetails": None,
            }
            return base | overrides

        expected_data = spark.createDataFrame(
            (
                generate_expected_data_row(casenodeid="1", caseuniqueid="refA", casespecialism="Some Specialism"),
                generate_expected_data_row(
                    casenodeid="2",
                    caseuniqueid="refB",
                    caseCreatedDate="2025-01-01T00:00:00",
                    appealdocscomplete="Romulan",
                    startdate="2026-01-01T00:00:00",
                    appealwithdrawndate="2027-01-01T00:00:00",
                    casedecisiondate="2028-01-01T00:00:00",
                    datenotrecoveredorderecovered="2029-01-01T00:00:00",
                    daterecovered="2030-01-01T00:00:00",
                    originalcasedecisiondate="2031-01-01T00:00:00",
                    floorspaceinsquaremetres="5",
                    costsappliedforindicator="10",
                    procedureappellant="Bob Vance",
                    isthesitewithinanaonb="No",
                    procedurelpa="Some LPA",
                    inspectorneedtoentersite="Perchance",
                    sitegridreferenceeasting="Some easting",
                    sitegridreferencenorthing="Some northing",
                    sitewithinsssi="Some SSSI",
                    level_code="Some level",
                ),
                generate_expected_data_row(
                    casenodeid="3",
                    caseuniqueid="refC",
                    questionnairedue="Klingon",
                    questionnairereceived="Cardassian",
                    interestedpartyrepsduedate="2032-01-01T00:00:00",
                    proofsdue="Vulcan",
                    importantinformation="Makarov knows Yuri",
                ),
                generate_expected_data_row(casenodeid="4", caseuniqueid="refD", siteviewablefromroad="Kelpian"),
                generate_expected_data_row(
                    casenodeid="5",
                    caseuniqueid="refE",
                    issueDateOfEnforcementNotice=datetime(2033, 1, 1),
                    effectiveDateOfEnforcementNotice=datetime(2034, 1, 1),
                ),
                generate_expected_data_row(
                    casenodeid="6",
                    caseuniqueid="refF",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "A", "groundForAppealStartDate": None}],
                ),
                generate_expected_data_row(casenodeid="7", caseuniqueid="refG"),
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("subtype", StringType(), True),
                    StructField("modifydate", StringType(), True),
                    StructField("abbreviation", StringType(), True),
                    StructField("casereference", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("caseofficerid", StringType(), True),
                    StructField("caseofficerlogin", StringType(), True),
                    StructField("caseofficername", StringType(), True),
                    StructField("coemailaddress", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                    StructField("casespecialism", StringType(), True),
                    StructField("caseCreatedDate", StringType(), True),
                    StructField("appealdocscomplete", StringType(), True),
                    StructField("startdate", StringType(), True),
                    StructField("appealwithdrawndate", StringType(), True),
                    StructField("casedecisiondate", StringType(), True),
                    StructField("datenotrecoveredorderecovered", StringType(), True),
                    StructField("daterecovered", StringType(), True),
                    StructField("originalcasedecisiondate", StringType(), True),
                    StructField("questionnairedue", StringType(), True),
                    StructField("questionnairereceived", StringType(), True),
                    StructField("interestedpartyrepsduedate", StringType(), True),
                    StructField("proofsdue", StringType(), True),
                    StructField("siteviewablefromroad", StringType(), True),
                    StructField("floorspaceinsquaremetres", StringType(), True),
                    StructField("costsappliedforindicator", StringType(), True),
                    StructField("procedureappellant", StringType(), True),
                    StructField("isthesitewithinanaonb", StringType(), True),
                    StructField("procedurelpa", StringType(), True),
                    StructField("inspectorneedtoentersite", StringType(), True),
                    StructField("sitegridreferenceeasting", StringType(), True),
                    StructField("sitegridreferencenorthing", StringType(), True),
                    StructField("sitewithinsssi", StringType(), True),
                    StructField("importantinformation", StringType(), True),
                    StructField("level_code", StringType(), True),
                    StructField("issueDateOfEnforcementNotice", TimestampType(), True),
                    StructField("effectiveDateOfEnforcementNotice", TimestampType(), True),
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
                        True,
                    ),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_base_table(
                horizon_cases,
                case_specialisms,
                case_dates,
                case_document_dates_dates,
                case_site_strings,
                addadditional_data,
                additional_fields,
                horizon_notice_dates,
                horizon_appeal_grounds,
            )
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_type_of_level_table(self):
        """
        add_typeoflevel AS (
        SELECT b.*, ctl.name AS allocationLevel, ctl.band AS allocationBand
        FROM base b
        LEFT JOIN ctl ON b.level_code = ctl.name
        ),
        """
        # add_typeoflevel
        spark = PytestSparkSessionUtil().get_spark_session()
        base_table = spark.createDataFrame(
            (
                {"colA": "a", "colB": "b", "colC": "c", "level_code": "codeA"},
                {"colA": "d", "colB": "e", "colC": "f", "level_code": "codeB"},
                {"colA": "g", "colB": "h", "colC": "i", "level_code": "codeC"},
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("colB", StringType(), True),
                    StructField("colC", StringType(), True),
                    StructField("level_code", StringType(), True),
                ]
            ),
        )

        type_of_level_data = spark.createDataFrame(
            (
                {"colD": "1", "colE": "2", "colF": "3", "name": "codeA", "band": "band1"},
                {"colD": "4", "colE": "5", "colF": "6", "name": "codeB", "band": "band2"},
                {"colD": "7", "colE": "8", "colF": "9", "name": "codeC", "band": "band3"},
            ),
            schema=StructType(
                [
                    StructField("colD", StringType(), True),
                    StructField("colE", StringType(), True),
                    StructField("colF", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("band", StringType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {"colA": "a", "colB": "b", "colC": "c", "level_code": "codeA", "allocationLevel": "codeA", "allocationBand": "band1"},
                {"colA": "d", "colB": "e", "colC": "f", "level_code": "codeB", "allocationLevel": "codeB", "allocationBand": "band2"},
                {"colA": "g", "colB": "h", "colC": "i", "level_code": "codeC", "allocationLevel": "codeC", "allocationBand": "band3"},
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("colB", StringType(), True),
                    StructField("colC", StringType(), True),
                    StructField("level_code", StringType(), True),
                    StructField("allocationLevel", StringType(), True),
                    StructField("allocationBand", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_type_of_level_table(base_table, type_of_level_data)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_planning(self):
        """
        add_planning AS (
        SELECT
            t.*,
            pas.lpaapplicationreference  AS applicationReference,
            pas.planningapplicationtype  AS typeOfPlanningApplication,
            pad.dateofapplication        AS applicationDate,
            pad.dateoflpadecision        AS applicationDecisionDate,
            CASE hmu.applicationMadeUnderSection
            WHEN '191' THEN 'existing-development'
            WHEN '192' THEN 'proposed-use-of-a-development'
            WHEN '26H' THEN 'proposed-changes-to-a-listed-building'
            ELSE NULL
            END AS applicationMadeUnderActSection
        FROM add_typeoflevel t
        LEFT JOIN pas_1row pas ON t.casenodeid = pas.casenodeid
        LEFT JOIN pad_1row pad ON t.casenodeid = pad.casenodeid
        LEFT JOIN hmu_1row hmu ON t.casenodeid = hmu.casenodeid
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        aad_type_of_level_data = spark.createDataFrame(
            (
                {"colA": "a", "colB": "b", "casenodeid": "1", "level_code": "codeA", "allocationLevel": "codeA", "allocationBand": "band1"},
                {"colA": "d", "colB": "e", "casenodeid": "2", "level_code": "codeB", "allocationLevel": "codeB", "allocationBand": "band2"},
                {"colA": "g", "colB": "h", "casenodeid": "3", "level_code": "codeC", "allocationLevel": "codeC", "allocationBand": "band3"},
                {
                    "colA": "i",
                    "colB": "j",
                    "casenodeid": "4",
                    "level_code": "codeC",
                    "allocationLevel": "codeC",
                    "allocationBand": "band3",
                },  # Unmatched
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("colB", StringType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("level_code", StringType(), True),
                    StructField("allocationLevel", StringType(), True),
                    StructField("allocationBand", StringType(), True),
                ]
            ),
        )
        planning_app_strings = spark.createDataFrame(
            (
                {"pasColA": 1, "casenodeid": "1", "lpaapplicationreference": "Obi-wan", "planningapplicationtype": "typeA"},
                {"pasColA": 2, "casenodeid": "2", "lpaapplicationreference": "Anakin", "planningapplicationtype": "typeB"},
                {"pasColA": 3, "casenodeid": "3", "lpaapplicationreference": "Ahsoka", "planningapplicationtype": "typeC"},
            ),
            schema=StructType(
                [
                    StructField("pasColA", StringType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("lpaapplicationreference", StringType(), True),
                    StructField("planningapplicationtype", StringType(), True),
                ]
            ),
        )
        planning_app_dates = spark.createDataFrame(
            (
                {"padColA": 1, "casenodeid": "1", "dateofapplication": datetime(2020, 1, 1), "dateoflpadecision": datetime(2021, 1, 1)},
                {"padColA": 2, "casenodeid": "2", "dateofapplication": datetime(2022, 1, 1), "dateoflpadecision": datetime(2023, 1, 1)},
                {"padColA": 3, "casenodeid": "3", "dateofapplication": datetime(2024, 1, 1), "dateoflpadecision": datetime(2025, 1, 1)},
            ),
            schema=StructType(
                [
                    StructField("padColA", StringType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("dateofapplication", TimestampType(), True),
                    StructField("dateoflpadecision", TimestampType(), True),
                ]
            ),
        )
        made_under_section_data = spark.createDataFrame(
            (
                {"hmuColA": 1, "casenodeid": "1", "applicationMadeUnderSection": "191"},
                {"hmuColA": 2, "casenodeid": "2", "applicationMadeUnderSection": "192"},
                {"hmuColA": 3, "casenodeid": "3", "applicationMadeUnderSection": "26H"},
                {"hmuColA": 3, "casenodeid": "3", "applicationMadeUnderSection": "Unmatched"},
            ),
            schema=StructType(
                [
                    StructField("hmuColA", StringType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("applicationMadeUnderSection", StringType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": "a",
                    "colB": "b",
                    "casenodeid": "1",
                    "level_code": "codeA",
                    "allocationLevel": "codeA",
                    "allocationBand": "band1",
                    "applicationReference": "Obi-wan",
                    "typeOfPlanningApplication": "typeA",
                    "applicationDate": datetime(2020, 1, 1, 0, 0),
                    "applicationDecisionDate": datetime(2021, 1, 1, 0, 0),
                    "applicationMadeUnderActSection": "existing-development",
                },
                {
                    "colA": "d",
                    "colB": "e",
                    "casenodeid": "2",
                    "level_code": "codeB",
                    "allocationLevel": "codeB",
                    "allocationBand": "band2",
                    "applicationReference": "Anakin",
                    "typeOfPlanningApplication": "typeB",
                    "applicationDate": datetime(2022, 1, 1, 0, 0),
                    "applicationDecisionDate": datetime(2023, 1, 1, 0, 0),
                    "applicationMadeUnderActSection": "proposed-use-of-a-development",
                },
                {
                    "colA": "g",
                    "colB": "h",
                    "casenodeid": "3",
                    "level_code": "codeC",
                    "allocationLevel": "codeC",
                    "allocationBand": "band3",
                    "applicationReference": "Ahsoka",
                    "typeOfPlanningApplication": "typeC",
                    "applicationDate": datetime(2024, 1, 1, 0, 0),
                    "applicationDecisionDate": datetime(2025, 1, 1, 0, 0),
                    "applicationMadeUnderActSection": None,
                },
                {
                    "colA": "g",
                    "colB": "h",
                    "casenodeid": "3",
                    "level_code": "codeC",
                    "allocationLevel": "codeC",
                    "allocationBand": "band3",
                    "applicationReference": "Ahsoka",
                    "typeOfPlanningApplication": "typeC",
                    "applicationDate": datetime(2024, 1, 1, 0, 0),
                    "applicationDecisionDate": datetime(2025, 1, 1, 0, 0),
                    "applicationMadeUnderActSection": "proposed-changes-to-a-listed-building",
                },
                {
                    "colA": "i",
                    "colB": "j",
                    "casenodeid": "4",
                    "level_code": "codeC",
                    "allocationLevel": "codeC",
                    "allocationBand": "band3",
                    "applicationReference": None,
                    "typeOfPlanningApplication": None,
                    "applicationDate": None,
                    "applicationDecisionDate": None,
                    "applicationMadeUnderActSection": None,
                },
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("colB", StringType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("level_code", StringType(), True),
                    StructField("allocationLevel", StringType(), True),
                    StructField("allocationBand", StringType(), True),
                    StructField("applicationReference", StringType(), True),
                    StructField("typeOfPlanningApplication", StringType(), True),
                    StructField("applicationDate", TimestampType(), True),
                    StructField("applicationDecisionDate", TimestampType(), True),
                    StructField("applicationMadeUnderActSection", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_planning(
                aad_type_of_level_data, planning_app_strings, planning_app_dates, made_under_section_data
            )
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_adverts(self):
        """
        add_adverts AS (
        SELECT
            p.*,
            haa.advertAttributeKey,
            haa.advertType,
            haa.setRowNumber,
            haa.publicSafetyLpaIndicator,
            haa.amenityLPAIndicator,
            haa.publicSafetyGroundOutcome,
            haa.amenityGroundOutcome,
            haa.advertInPosition,
            haa.siteOnHighwayLand
        FROM add_planning p
        LEFT JOIN haa_1row haa
            ON p.caseuniqueid = haa.caseuniqueid
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        add_planning = spark.createDataFrame(
            (
                {"colA": "a", "caseuniqueid": "1"},
                {"colA": "b", "caseuniqueid": "2"},
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                ]
            ),
        )
        advert_attributes = spark.createDataFrame(
            (
                {
                    "caseuniqueid": "1",
                    "advertAttributeKey": "keyA",
                    "advertType": "typeA",
                    "setRowNumber": 1,
                    "publicSafetyLpaIndicator": "A",
                    "amenityLPAIndicator": "X",
                    "publicSafetyGroundOutcome": "rejected",
                    "amenityGroundOutcome": "rejected",
                    "advertInPosition": True,
                    "siteOnHighwayLand": True,
                },
            ),
            schema=StructType(
                [
                    StructField("caseuniqueid", StringType(), True),
                    StructField("advertAttributeKey", StringType(), True),
                    StructField("advertType", StringType(), True),
                    StructField("setRowNumber", IntegerType(), True),
                    StructField("publicSafetyLpaIndicator", StringType(), True),
                    StructField("amenityLPAIndicator", StringType(), True),
                    StructField("publicSafetyGroundOutcome", StringType(), True),
                    StructField("amenityGroundOutcome", StringType(), True),
                    StructField("advertInPosition", BooleanType(), True),
                    StructField("siteOnHighwayLand", BooleanType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "advertAttributeKey": "keyA",
                    "advertType": "typeA",
                    "setRowNumber": 1,
                    "publicSafetyLpaIndicator": "A",
                    "amenityLPAIndicator": "X",
                    "publicSafetyGroundOutcome": "rejected",
                    "amenityGroundOutcome": "rejected",
                    "advertInPosition": True,
                    "siteOnHighwayLand": True,
                },
                {
                    "colA": "b",
                    "caseuniqueid": "2",
                    "advertAttributeKey": None,
                    "advertType": None,
                    "setRowNumber": None,
                    "publicSafetyLpaIndicator": None,
                    "amenityLPAIndicator": None,
                    "publicSafetyGroundOutcome": None,
                    "amenityGroundOutcome": None,
                    "advertInPosition": None,
                    "siteOnHighwayLand": None,
                },
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("advertAttributeKey", StringType(), True),
                    StructField("advertType", StringType(), True),
                    StructField("setRowNumber", IntegerType(), True),
                    StructField("publicSafetyLpaIndicator", StringType(), True),
                    StructField("amenityLPAIndicator", StringType(), True),
                    StructField("publicSafetyGroundOutcome", StringType(), True),
                    StructField("amenityGroundOutcome", StringType(), True),
                    StructField("advertInPosition", BooleanType(), True),
                    StructField("siteOnHighwayLand", BooleanType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_adverts(add_planning, advert_attributes)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_case_refs(self):
        """
        add_case_refs AS (
        SELECT
            p.*,
            lc.leadcasenodeid      AS leadCaseReference,
            cs2.processingstate    AS caseStatus,
            cs2.lpacode            AS lpaCode,
            cs2.linkedstatus       AS linkedCaseStatus,
            cs2.decision           AS caseDecisionOutcome,
            cs2.jurisdiction       AS jurisdiction,
            cs2.redetermined       AS redeterminedIndicator,
            cs2.developmenttype    AS developmentType,
            cs2.proceduretype      AS procedureType,
            ci.validity            AS caseValidationOutcome
        FROM add_adverts p
        LEFT JOIN lc_1row  lc  ON p.casenodeid   = lc.casenodeid
        LEFT JOIN cs2_1row cs2 ON p.casenodeid   = cs2.casenodeid
        LEFT JOIN ci_1row  ci  ON p.caseuniqueid = ci.appealrefnumber
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        add_adverts = spark.createDataFrame(
            (
                {"casenodeid": "1", "caseuniqueid": "id1", "colA": 1},
                {"casenodeid": "2", "caseuniqueid": "id2", "colA": 2},
            ),
            schema=StructType(
                [
                    StructField("colA", IntegerType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                ]
            ),
        )
        # lc_1row
        bis_lead_case = spark.createDataFrame(
            ({"casenodeid": 1, "leadcasenodeid": "A"},),
            schema=StructType(
                [
                    StructField("casenodeid", StringType(), True),
                    StructField("leadcasenodeid", StringType(), True),
                ]
            ),
        )
        # cs2_1row
        bis_case_strings = spark.createDataFrame(
            (
                {
                    "casenodeid": "1",
                    "processingstate": "stateA",
                    "lpacode": "lpaA",
                    "linkedstatus": "lsA",
                    "decision": "decisionA",
                    "jurisdiction": "jurisdictionA",
                    "redetermined": "redeterminedA",
                    "developmenttype": "typeA",
                    "proceduretype": "typeB",
                },
            ),
            schema=StructType(
                [
                    StructField("casenodeid", StringType(), True),
                    StructField("processingstate", StringType(), True),
                    StructField("lpacode", StringType(), True),
                    StructField("linkedstatus", StringType(), True),
                    StructField("decision", StringType(), True),
                    StructField("jurisdiction", StringType(), True),
                    StructField("redetermined", StringType(), True),
                    StructField("developmenttype", StringType(), True),
                    StructField("proceduretype", StringType(), True),
                ]
            ),
        )
        # ci_1row
        horizon_case_info = spark.createDataFrame(
            ({"validity": "valid", "appealrefnumber": "id1"},),
            schema=StructType(
                [
                    StructField("validity", StringType(), True),
                    StructField("appealrefnumber", StringType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": 1,
                    "casenodeid": "1",
                    "caseuniqueid": "id1",
                    "leadCaseReference": "A",
                    "caseStatus": "stateA",
                    "lpaCode": "lpaA",
                    "linkedCaseStatus": "lsA",
                    "caseDecisionOutcome": "decisionA",
                    "jurisdiction": "jurisdictionA",
                    "redeterminedIndicator": "redeterminedA",
                    "developmentType": "typeA",
                    "procedureType": "typeB",
                    "caseValidationOutcome": "valid",
                },
                {
                    "colA": 2,
                    "casenodeid": "2",
                    "caseuniqueid": "id2",
                    "leadCaseReference": None,
                    "caseStatus": None,
                    "lpaCode": None,
                    "linkedCaseStatus": None,
                    "caseDecisionOutcome": None,
                    "jurisdiction": None,
                    "redeterminedIndicator": None,
                    "developmentType": None,
                    "procedureType": None,
                    "caseValidationOutcome": None,
                },
            ),
            schema=StructType(
                [
                    StructField("colA", IntegerType(), True),
                    StructField("casenodeid", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("leadCaseReference", StringType(), True),
                    StructField("caseStatus", StringType(), True),
                    StructField("lpaCode", StringType(), True),
                    StructField("linkedCaseStatus", StringType(), True),
                    StructField("caseDecisionOutcome", StringType(), True),
                    StructField("jurisdiction", StringType(), True),
                    StructField("redeterminedIndicator", StringType(), True),
                    StructField("developmentType", StringType(), True),
                    StructField("procedureType", StringType(), True),
                    StructField("caseValidationOutcome", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_case_refs(add_adverts, bis_lead_case, bis_case_strings, horizon_case_info)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_dates(self):
        """
        add_dates AS (
        SELECT
            c.*,
            cdh.validitystatusdate        AS caseValidationDate,
            scd.datecostsreportdespatched AS dateCostsReportDespatched
        FROM add_case_refs c
        LEFT JOIN cdh_1row cdh ON c.caseuniqueid = cdh.appealrefnumber
        LEFT JOIN scd_1row scd ON c.caseuniqueid = scd.appealrefnumber
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()

        add_case_refs = spark.createDataFrame(
            ({"colA": 1, "caseuniqueid": "a"}, {"colA": 2, "caseuniqueid": "b"}),
            schema=StructType(
                [
                    StructField("colA", IntegerType(), True),
                    StructField("caseuniqueid", StringType(), True),
                ]
            ),
        )
        # cdh_1row
        horizon_case_dates = spark.createDataFrame(
            ({"appealrefnumber": "a", "validitystatusdate": datetime(2025, 1, 1)},),
            schema=StructType(
                [
                    StructField("appealrefnumber", StringType(), True),
                    StructField("validitystatusdate", TimestampType(), True),
                ]
            ),
        )
        # scd_1row
        specialist_case_dates = spark.createDataFrame(
            ({"appealrefnumber": "a", "datecostsreportdespatched": datetime(2026, 1, 1)},),
            schema=StructType(
                [
                    StructField("appealrefnumber", StringType(), True),
                    StructField("datecostsreportdespatched", TimestampType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": 1,
                    "caseuniqueid": "a",
                    "caseValidationDate": datetime(2025, 1, 1, 0, 0),
                    "dateCostsReportDespatched": datetime(2026, 1, 1, 0, 0),
                },
                {"colA": 2, "caseuniqueid": "b", "caseValidationDate": None, "dateCostsReportDespatched": None},
            ),
            schema=StructType(
                [
                    StructField("colA", IntegerType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("caseValidationDate", TimestampType(), True),
                    StructField("dateCostsReportDespatched", TimestampType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_dates(add_case_refs, horizon_case_dates, specialist_case_dates)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_aad(self):
        """
        add_aad AS (
        SELECT
            d.*,
            aad.developmentorallegation     AS originalDevelopmentDescription,
            aad.appellantcommentssubmitted  AS appellantCommentsSubmittedDate,
            aad.appellantstatementsubmitted AS appellantStatementSubmittedDate,
            aad.lpacommentssubmitted        AS lpaCommentsSubmittedDate,
            aad.lpaproofssubmitted          AS lpaProofsSubmittedDate,
            aad.lpastatementsubmitted       AS lpaStatementSubmittedDate,
            aad.sitenoticesent              AS siteNoticesSentDate,
            aad.statementsdue               AS statementDueDate,
            aad.numberofresidences          AS numberOfResidencesNetChange
        FROM add_dates d
        LEFT JOIN aad_1row aad ON d.caseuniqueid = aad.appealrefnumber
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        add_dates = spark.createDataFrame(
            (
                {"colA": "a", "caseuniqueid": 1},
                {"colA": "b", "caseuniqueid": 2},
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", IntegerType(), True),
                ]
            ),
        )
        # aad_1row
        appeals_additional_data = spark.createDataFrame(
            (
                {
                    "appealrefnumber": 1,
                    "developmentorallegation": "developmentorallegation",
                    "appellantcommentssubmitted": "appellantcommentssubmitted",
                    "appellantstatementsubmitted": "appellantstatementsubmitted",
                    "lpacommentssubmitted": "lpacommentssubmitted",
                    "lpaproofssubmitted": "lpaproofssubmitted",
                    "lpastatementsubmitted": "lpastatementsubmitted",
                    "sitenoticesent": "sitenoticesent",
                    "statementsdue": "statementsdue",
                    "numberofresidences": "numberofresidences",
                },
            ),
            schema=StructType(
                [
                    StructField("appealrefnumber", IntegerType(), True),
                    StructField("developmentorallegation", StringType(), True),
                    StructField("appellantcommentssubmitted", StringType(), True),
                    StructField("appellantstatementsubmitted", StringType(), True),
                    StructField("lpacommentssubmitted", StringType(), True),
                    StructField("lpaproofssubmitted", StringType(), True),
                    StructField("lpastatementsubmitted", StringType(), True),
                    StructField("sitenoticesent", StringType(), True),
                    StructField("statementsdue", StringType(), True),
                    StructField("numberofresidences", StringType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": "a",
                    "caseuniqueid": 1,
                    "originalDevelopmentDescription": "developmentorallegation",
                    "appellantCommentsSubmittedDate": "appellantcommentssubmitted",
                    "appellantStatementSubmittedDate": "appellantstatementsubmitted",
                    "lpaCommentsSubmittedDate": "lpacommentssubmitted",
                    "lpaProofsSubmittedDate": "lpaproofssubmitted",
                    "lpaStatementSubmittedDate": "lpastatementsubmitted",
                    "siteNoticesSentDate": "sitenoticesent",
                    "statementDueDate": "statementsdue",
                    "numberOfResidencesNetChange": "numberofresidences",
                },
                {
                    "colA": "b",
                    "caseuniqueid": 2,
                    "originalDevelopmentDescription": None,
                    "appellantCommentsSubmittedDate": None,
                    "appellantStatementSubmittedDate": None,
                    "lpaCommentsSubmittedDate": None,
                    "lpaProofsSubmittedDate": None,
                    "lpaStatementSubmittedDate": None,
                    "siteNoticesSentDate": None,
                    "statementDueDate": None,
                    "numberOfResidencesNetChange": None,
                },
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", IntegerType(), True),
                    StructField("originalDevelopmentDescription", StringType(), True),
                    StructField("appellantCommentsSubmittedDate", StringType(), True),
                    StructField("appellantStatementSubmittedDate", StringType(), True),
                    StructField("lpaCommentsSubmittedDate", StringType(), True),
                    StructField("lpaProofsSubmittedDate", StringType(), True),
                    StructField("lpaStatementSubmittedDate", StringType(), True),
                    StructField("siteNoticesSentDate", StringType(), True),
                    StructField("statementDueDate", StringType(), True),
                    StructField("numberOfResidencesNetChange", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_aad(add_dates, appeals_additional_data)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_add_procedure(self):
        """
        add_procedure AS (
        SELECT a.*, tp.proccode AS caseProcedure
        FROM add_aad a
        LEFT JOIN tp ON a.procedureType = tp.name
        )
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        add_aad = spark.createDataFrame(
            (
                {"colA": "a", "procedureType": "typeA"},
                {"colA": "b", "procedureType": "typeB"},
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("procedureType", StringType(), True),
                ]
            ),
        )
        type_of_procedure = spark.createDataFrame(
            ({"name": "typeA", "proccode": "1"},),
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("proccode", StringType(), True),
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            ({"colA": "a", "procedureType": "typeA", "caseProcedure": "1"}, {"colA": "b", "procedureType": "typeB", "caseProcedure": None}),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("procedureType", StringType(), True),
                    StructField("caseProcedure", StringType(), True),
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_add_procedure(add_aad, type_of_procedure)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__generate_final_table(self):
        """
        SELECT *
        FROM (
        SELECT a.*,
                ROW_NUMBER() OVER (
                PARTITION BY a.caseuniqueid
                ORDER BY a.expected_from DESC,
                            a.modified_datetime DESC,
                            a.ingested_datetime DESC,
                            a.file_id DESC
                ) AS rn_final
        FROM add_procedure a
        ) z
        WHERE rn_final = 1
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        expected_from_base = datetime(2025, 1, 1)
        modified_datetime_base = datetime(2026, 1, 1)
        ingested_datetime_base = datetime(2027, 1, 1)

        expected_from_inc = expected_from_base + timedelta(days=1)
        modified_datetime_inc = modified_datetime_base + timedelta(days=1)
        ingested_datetime_inc = ingested_datetime_base + timedelta(days=1)
        add_procedure = spark.createDataFrame(
            (
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_base,
                    "modified_datetime": modified_datetime_base,
                    "ingested_datetime": ingested_datetime_base,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_base,
                    "modified_datetime": modified_datetime_base,
                    "ingested_datetime": ingested_datetime_inc,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_base,
                    "modified_datetime": modified_datetime_inc,
                    "ingested_datetime": ingested_datetime_base,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_base,
                    "modified_datetime": modified_datetime_inc,
                    "ingested_datetime": ingested_datetime_inc,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_inc,
                    "modified_datetime": modified_datetime_base,
                    "ingested_datetime": ingested_datetime_base,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_inc,
                    "modified_datetime": modified_datetime_base,
                    "ingested_datetime": ingested_datetime_inc,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_inc,
                    "modified_datetime": modified_datetime_inc,
                    "ingested_datetime": ingested_datetime_base,
                    "level_code": "x",
                },
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "file_id": "1",
                    "expected_from": expected_from_inc,
                    "modified_datetime": modified_datetime_inc,
                    "ingested_datetime": ingested_datetime_inc,
                    "level_code": "x",
                },
                {
                    "colA": "b",
                    "caseuniqueid": "2",
                    "file_id": "2",
                    "expected_from": expected_from_base,
                    "modified_datetime": modified_datetime_base,
                    "ingested_datetime": ingested_datetime_base,
                    "level_code": "x",
                },
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("file_id", StringType(), True),
                    StructField("level_code", StringType(), True),  # This column should be dropped
                ]
            ),
        )
        expected_data = spark.createDataFrame(
            (
                {
                    "colA": "a",
                    "caseuniqueid": "1",
                    "expected_from": datetime(2025, 1, 2, 0, 0),
                    "modified_datetime": datetime(2026, 1, 2, 0, 0),
                    "ingested_datetime": datetime(2027, 1, 2, 0, 0),
                    "file_id": "1",
                    "rn_final": 1,
                },
                {
                    "colA": "b",
                    "caseuniqueid": "2",
                    "expected_from": datetime(2025, 1, 1, 0, 0),
                    "modified_datetime": datetime(2026, 1, 1, 0, 0),
                    "ingested_datetime": datetime(2027, 1, 1, 0, 0),
                    "file_id": "2",
                    "rn_final": 1,
                },
            ),
            schema=StructType(
                [
                    StructField("colA", StringType(), True),
                    StructField("caseuniqueid", StringType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("file_id", StringType(), True),
                    StructField("rn_final", IntegerType(), False),
                    StructField("preserveGrantLoan", BooleanType(), True),  # Should be added but be empty
                    StructField("consultHistoricEngland", BooleanType(), True),  # Should be added but be empty
                ]
            ),
        )
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            actual_data = AppealS78StandardisationProcess().generate_final_table(add_procedure)
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__process(self):
        with mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None):
            inst = AppealS78StandardisationProcess()
            functions_to_patch = (
                inst.generate_add_aad,
                inst.generate_add_adverts,
                inst.generate_add_case_refs,
                inst.generate_add_dates,
                inst.generate_add_planning,
                inst.generate_add_dates,
                inst.generate_add_procedure,
                inst.generate_base_table,
                inst.generate_type_of_level_table,
                inst.generate_final_table,
            )
            with ExitStack() as stack:
                for return_value, function in enumerate(functions_to_patch):
                    method = function.__name__
                    stack.enter_context(mock.patch.object(inst, method, return_value=return_value))
                expected_result = getattr(inst, "generate_final_table").return_value
                actual_result = inst.process()
                called_functions_map = {x.__name__: getattr(inst, x.__name__).called for x in functions_to_patch}
                uncalled_functions = [k for k, v in called_functions_map.items() if not v]
                assert not uncalled_functions, (
                    f"The following methods of AppealS78StandardisationProcess were not called by process() but were expected {uncalled_functions}"
                )
                assert expected_result == actual_result
