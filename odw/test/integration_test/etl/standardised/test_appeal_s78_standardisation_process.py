import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType
from odw.core.etl.transformation.standardised.appeal_s78_standardisation_process import AppealS78StandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


# odw_standardised_db.horizoncases_s78
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


def horizon_cases_schema():
    return StructType(
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
    )


# odw_standardised_db.cases_specialisms
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


# odw_standardised_db.vw_case_dates
def generate_case_dates_schema():
     return StructType(
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
    )


def generate_case_specialisms_row():
    return StructType(
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
    )


class TestAppealS78StandardisationProcess(ETLTestCase):
    def test__appeal_s78_standardisation_process__run__with_no_existing_data():
        pass

    def test__appeal_s78_standardisation_process__run__with_existing_data():
        pass
