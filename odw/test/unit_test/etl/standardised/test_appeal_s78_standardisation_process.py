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

    def test__appeal_s78_standardisation_process__load_horizoncases_s78(self):
        override_table_name = "t_as78sp_lhcs78"
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_horizoncases_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": "",
                "subtype": "",
                "modifydate": "",
                "abbreviation": "",
                "casereference": "",
                "caseuniqueid": "",
                "caseofficerid": "",
                "caseofficerlogin": "",
                "caseofficername": "",
                "coemailaddress": "",
                "input_file": "",
                "ingested_by_process_name": "some_process",
                "modified_datetime": None,
                "modified_by_process_name": "some_process",
                "entity_name": "HorizonCases_s78",
                "file_id": "",
            }
            return base | overrides

        def generate_expected_horizoncases_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": "",
                "subtype": "",
                "modifydate": "",
                "abbreviation": "",
                "casereference": "",
                "caseuniqueid": "1",
                "caseofficerid": "",
                "caseofficerlogin": "",
                "caseofficername": "",
                "coemailaddress": "",
                "input_file": "",
                "ingested_by_process_name": "some_process",
                "modified_datetime": None,
                "modified_by_process_name": "some_process",
                "entity_name": "HorizonCases_s78",
                "file_id": "b",
                "rn": 1,
            }
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
                generate_horizoncases_row(caseuniqueid="1"),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_horizoncases_row(
                    caseuniqueid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_horizoncases_row(caseuniqueid="2"),
                generate_horizoncases_row(caseuniqueid="3"),
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
        self.write_existing_table(
            spark,
            raw_data,
            override_table_name,
            "odw_standardised_db",
            "odw-standardised",
            override_table_name,
            "overwrite",
        )
        expected_data = spark.createDataFrame(
            (
                generate_expected_horizoncases_row(
                    caseuniqueid="1",
                    ingested_datetime=ingested_datetime_inc,
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                ),
                generate_expected_horizoncases_row(caseuniqueid="2", ingested_datetime=None, expected_from=None, modified_datetime=None),
                generate_expected_horizoncases_row(caseuniqueid="3", ingested_datetime=None, expected_from=None, modified_datetime=None),
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
                    StructField("rn", IntegerType(), False),
                ]
            ),
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_HORIZON_CASES_S78", override_table_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_horizoncases_s78()
            assert_dataframes_equal(expected_data, actual_data)

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
        override_table_name = "t_as78sp_lvwcd"
        spark = PytestSparkSessionUtil().get_spark_session()

        def generate_case_dates_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": "",
                "receiptdate": "",
                "startdate": "",
                "appealdocscomplete": "",
                "callindate": "",
                "datereceivedfromcallinauthority": "",
                "bespoketargetdate": "",
                "daterecovered": "",
                "datenotrecoveredorderecovered": "",
                "decisionsubmitdate": "",
                "noticewithdrawndate": "",
                "appealwithdrawndate": "",
                "casedecisiondate": "",
                "appealturnedawaydate": "",
                "appeallapseddate": "",
                "casecloseddate": "",
                "proceduredetermineddate": "",
                "originalcasedecisiondate": "",
                "planningguaranteedate": "",
                "datedecisionreportreceivedinpins": "",
                "datesenttoreader": "",
                "datereturnedfromreader": "",
                "datepublicationprocedurecompleted": "",
                "datecostsreportdespatched": "",
                "orderrejectedorreturned": "",
                "input_file": "",
                "ingested_by_process_name": "",
                "modified_datetime": None,
                "modified_by_process_name": "",
                "entity_name": "",
                "file_id": "",
            }
            return base | overrides

        def generate_expected_case_dates_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casenodeid": "1",
                "receiptdate": "",
                "startdate": "",
                "appealdocscomplete": "",
                "callindate": "",
                "datereceivedfromcallinauthority": "",
                "bespoketargetdate": "",
                "daterecovered": "",
                "datenotrecoveredorderecovered": "",
                "decisionsubmitdate": "",
                "noticewithdrawndate": "",
                "appealwithdrawndate": "",
                "casedecisiondate": "",
                "appealturnedawaydate": "",
                "appeallapseddate": "",
                "casecloseddate": "",
                "proceduredetermineddate": "",
                "originalcasedecisiondate": "",
                "planningguaranteedate": "",
                "datedecisionreportreceivedinpins": "",
                "datesenttoreader": "",
                "datereturnedfromreader": "",
                "datepublicationprocedurecompleted": "",
                "datecostsreportdespatched": "",
                "orderrejectedorreturned": "",
                "input_file": "",
                "ingested_by_process_name": "",
                "modified_datetime": None,
                "modified_by_process_name": "",
                "entity_name": "",
                "file_id": "b",
                "rn": 1,
            }
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
                generate_case_dates_row(casenodeid="1"),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_base,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_base,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_base,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_base,
                ),
                generate_case_dates_row(
                    casenodeid="1",
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                    ingested_datetime=ingested_datetime_inc,
                    file_id=file_id_inc,
                ),
                generate_case_dates_row(casenodeid="2"),
                generate_case_dates_row(casenodeid="3"),
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
        self.write_existing_table(
            spark,
            raw_data,
            override_table_name,
            "odw_standardised_db",
            "odw-standardised",
            override_table_name,
            "overwrite",
        )
        expected_data = spark.createDataFrame(
            (
                generate_expected_case_dates_row(
                    casenodeid="1",
                    ingested_datetime=ingested_datetime_inc,
                    expected_from=expected_from_inc,
                    modified_datetime=modified_datetime_inc,
                ),
                generate_expected_case_dates_row(casenodeid="2", ingested_datetime=None, expected_from=None, modified_datetime=None),
                generate_expected_case_dates_row(casenodeid="3", ingested_datetime=None, expected_from=None, modified_datetime=None),
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
                    StructField("rn", IntegerType(), False),
                ]
            ),
        )
        with (
            mock.patch.object(AppealS78StandardisationProcess, "STANDARDISED_HORIZON_CASE_DATES", override_table_name),
            mock.patch.object(AppealS78StandardisationProcess, "__init__", return_value=None),
        ):
            actual_data = AppealS78StandardisationProcess()._load_standardised_vw_case_dates()
            assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_s78_standardisation_process__load_casedocumentdatesdates(self):
        """
        cdd_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cdd x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_casesitestrings(self):
        """
        css_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM css x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_typeofprocedure(self):
        """
        add_procedure AS (
        SELECT a.*, tp.proccode AS caseProcedure
        FROM add_aad a
        LEFT JOIN tp ON a.procedureType = tp.name
        )
        """

    def test__appeal_s78_standardisation_process__load_vw_addadditionaldata(self):
        """
        aad_old_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM aad_old x
        ) t WHERE rn = 1
        ),
        """

    def test__appeal_s78_standardisation_process__load_vw_additionalfields(self):
        """
        af_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM af x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_advert_attributes(self):
        """
        haa_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.caseuniqueid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM haa x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_TypeOfLevel(self):
        """
        add_typeoflevel AS (
        SELECT b.*, ctl.name AS allocationLevel, ctl.band AS allocationBand
        FROM base b
        LEFT JOIN ctl ON b.level_code = ctl.name
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_specialist_case_dates(self):
        """
        scd_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM scd x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_PlanningAppStrings(self):
        """
        pas_1row AS (
        SELECT * FROM (
            SELECT p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.casenodeid
                    ORDER BY p.expected_from DESC, p.modified_datetime DESC, p.ingested_datetime DESC, p.file_id DESC
                ) rn
            FROM pas p
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_PlanningAppDates(self):
        """
        pad_1row AS (
        SELECT * FROM (
            SELECT p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.casenodeid
                    ORDER BY p.expected_from DESC, p.modified_datetime DESC, p.ingested_datetime DESC, p.file_id DESC
                ) rn
            FROM pad p
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_BIS_LeadCase(self):
        """
        lc_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM lc x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_CaseStrings(self):
        """
        cs2_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cs2 x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_case_info(self):
        """
        ci_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM ci x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_case_dates(self):
        """
        cdh_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cdh x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_appeals_additional_data(self):
        """
        ad_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM aad x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_appeal_grounds(self):
        """
        hag_agg AS (
        SELECT
            g.casenodeid,
            sort_array(
            collect_list(
                named_struct(
                'appealGroundLetter', g.appealgroundletter,
                'groundForAppealStartDate', g.groundforappealstartdate
                )
            )
            ) AS enforcementAppealGroundsDetails
        FROM hag g
        GROUP BY g.casenodeid
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_notice_dates(self):
        """
        hnd_1row AS (
        SELECT * FROM (
            SELECT n.*,
                ROW_NUMBER() OVER (
                    PARTITION BY n.casenodeid
                    ORDER BY n.expected_from DESC,
                            n.setrownumber DESC,
                            n.ingested_datetime DESC,
                            n.file_id DESC
                ) rn
            FROM hnd n
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_application_made_under_section(self):
        """
        hmu_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC,
                            x.modified_datetime DESC,
                            x.ingested_datetime DESC,
                            x.file_id DESC
                ) rn
            FROM hmu x
        ) t WHERE rn = 1
        )
        """

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
