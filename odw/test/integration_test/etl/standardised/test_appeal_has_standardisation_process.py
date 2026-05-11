import mock
import pytest
import pyspark.sql.types as T
from odw.test.util.assertion import assert_etl_result_successful, assert_dataframes_equal
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.standardised.appeal_has_standardisation_process import AppealHasStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType
from datetime import datetime
from contextlib import ExitStack

pytestmark = pytest.mark.xfail(reason="Standardisation logic not implemented yet")


def horizon_cases_has_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "CaseNodeId": None,
        "subType": None,
        "modifyDate": datetime(2025, 1, 1),
        "Abbreviation": None,
        "caseReference": None,
        "caseUniqueId": None,
        "caseOfficerId": None,
        "caseOfficerLogin": None,
        "caseOfficerName": None,
        "coEmailAddress": None,
        "casSubType": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_cases_has_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("CaseNodeId", StringType(), True),
            StructField("subType", StringType(), True),
            StructField("modifyDate", TimestampType(), True),
            StructField("Abbreviation", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("caseUniqueId", StringType(), True),
            StructField("caseOfficerId", StringType(), True),
            StructField("caseOfficerLogin", StringType(), True),
            StructField("caseOfficerName", StringType(), True),
            StructField("coEmailAddress", StringType(), True),
            StructField("casSubType", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def cases_specialisms_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "casereference": None,
        "lastmodified": None,
        "casespecialism": None,
        "input_file": None,
        "ingested_by_process_name": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": "cases_specialisms",
        "file_id": None,
    }
    return base | overrides


def cases_specialisms_schema():
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


def vw_cases_dates_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
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
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": "vw_case_dates",
        "file_id": None,
    }
    return base | overrides


def vw_cases_dates_schema():
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


def case_document_dates_dates_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
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
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def case_document_dates_dates_schema():
    return StructType(
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
    )


def case_site_strings_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "casenodeid": None,
        "landuse": None,
        "areaofsite": None,
        "addressline1": None,
        "addressline2": None,
        "town": None,
        "county": None,
        "postcode": None,
        "country": None,
        "greenbelt": "Yes",
        "siteviewablefromroad": None,
        "input_file": None,
        "ingested_by_process_name": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def case_site_strings_schema():
    return StructType(
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
    )


def type_of_procedure_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "id": None,
        "displayorder": None,
        "name": None,
        "description": None,
        "proccode": None,
        "startdate": None,
        "enddate": None,
        "input_file": None,
        "ingested_by_process_name": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": "TypeOfProcedure",
        "file_id": None,
    }
    return base | overrides


def type_of_procedure_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("id", StringType(), True),
            StructField("displayorder", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("proccode", StringType(), True),
            StructField("startdate", StringType(), True),
            StructField("enddate", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def vw_additional_data_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
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
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": "vw_AddAdditionalData",
        "file_id": None,
    }
    return base | overrides


def vw_additional_data_schema():
    return StructType(
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
    )


def vw_additional_fields_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "casereference": None,
        "standardpriority": None,
        "importantinformation": None,
        "input_file": None,
        "ingested_by_process_name": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def vw_additional_fields_schema():
    return StructType(
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
    )


def horizon_advert_attributes_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "advertAttributeKey": None,
        "caseNodeId": None,
        "caseUniqueId": None,
        "setRowNumber": 1,
        "advertType": None,
        "publicSafetyLpaIndicator": None,
        "amenityLpaIndicator": None,
        "publicSafetyGroundOutcome": None,
        "amenityGroundOutcome": None,
        "advertInPosition": None,
        "siteOnHighwayLand": None,
        "advertdescription": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_advert_attributes_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("advertAttributeKey", StringType(), True),
            StructField("caseNodeId", IntegerType(), True),
            StructField("caseUniqueId", IntegerType(), True),
            StructField("setRowNumber", IntegerType(), True),
            StructField("advertType", StringType(), True),
            StructField("publicSafetyLpaIndicator", StringType(), True),
            StructField("amenityLpaIndicator", StringType(), True),
            StructField("publicSafetyGroundOutcome", StringType(), True),
            StructField("amenityGroundOutcome", StringType(), True),
            StructField("advertInPosition", StringType(), True),
            StructField("siteOnHighwayLand", StringType(), True),
            StructField("advertdescription", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def type_of_level_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "name": None,
        "band": None,
        "displayorder": 1,
        "id": 1,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def type_of_level_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("name", StringType(), True),
            StructField("band", StringType(), True),
            StructField("displayorder", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def horizon_specialist_case_dates_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "datecostsreportdespatched": None,
        "datedecisionreportreceivedinpins": None,
        "datepublicationprocedurecompleted": None,
        "datereturnedfromreader": None,
        "datesenttoreader": None,
        "orderrejectedorreturned": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_specialist_case_dates_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("appealrefnumber", StringType(), True),
            StructField("datecostsreportdespatched", StringType(), True),
            StructField("datedecisionreportreceivedinpins", StringType(), True),
            StructField("datepublicationprocedurecompleted", StringType(), True),
            StructField("datereturnedfromreader", StringType(), True),
            StructField("datesenttoreader", StringType(), True),
            StructField("orderrejectedorreturned", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def planning_app_strings_row(**overrides):
    base = {}
    return base | overrides


def planning_app_strings_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("casenodeid", StringType(), True),
            StructField("planningapplicationtype", StringType(), True),
            StructField("lpaapplicationreference", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def planning_app_dates_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "casenodeid": None,
        "dateoflpaddecision": None,
        "dateofapplication": None,
        "dateoflpadecision": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def planning_app_dates_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("casenodeid", StringType(), True),
            StructField("dateoflpaddecision", StringType(), True),
            StructField("dateofapplication", StringType(), True),
            StructField("dateoflpadecision", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def bis_lead_case_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "casenodeid": None,
        "leadcasenodeid": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def bis_lead_case_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("casenodeid", StringType(), True),
            StructField("leadcasenodeid", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def case_strings_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "casenodeid": None,
        "proceduretype": None,
        "decision": None,
        "linkedstatus": None,
        "lpacode": None,
        "lpaname": None,
        "caseworkreason": None,
        "casetype": None,
        "redetermined": None,
        "jurisdiction": None,
        "developmenttype": None,
        "bespokeindicator": None,
        "processingstate": None,
        "sourceindicator": None,
        "englandwalesindicator": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def case_strings_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("casenodeid", StringType(), True),
            StructField("proceduretype", StringType(), True),
            StructField("decision", StringType(), True),
            StructField("linkedstatus", StringType(), True),
            StructField("lpacode", StringType(), True),
            StructField("lpaname", StringType(), True),
            StructField("caseworkreason", StringType(), True),
            StructField("casetype", StringType(), True),
            StructField("redetermined", StringType(), True),
            StructField("jurisdiction", StringType(), True),
            StructField("developmenttype", StringType(), True),
            StructField("bespokeindicator", StringType(), True),
            StructField("processingstate", StringType(), True),
            StructField("sourceindicator", StringType(), True),
            StructField("englandwalesindicator", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def horizon_case_info_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "casereference": None,
        "validity": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_case_info_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("appealrefnumber", StringType(), True),
            StructField("casereference", StringType(), True),
            StructField("validity", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def horizon_case_dates_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "casereference": None,
        "validitystatusdate": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_case_dates_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("appealrefnumber", StringType(), True),
            StructField("casereference", StringType(), True),
            StructField("validitystatusdate", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def horizon_appeals_additional_data_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "lastmodified": None,
        "decisiondate": None,
        "decisionsubmitdate": None,
        "bespokeindicator": None,
        "bespoketargetdate": None,
        "processingstate": None,
        "linkstatus": None,
        "casecloseddate": None,
        "appellant": None,
        "pcsappealwithdrawndate": None,
        "numberofresidences": None,
        "appealsdocumentscomplete": None,
        "questionnairereceived": None,
        "lpaconditionsreceived": None,
        "lpaconditionsforwarded": None,
        "statementsdue": None,
        "lpastatementsubmitted": None,
        "lpastatementforwarded": None,
        "appellantstatementsubmitted": None,
        "appellantstatementforwarded": None,
        "finalcommentsdue": None,
        "lpacommentssubmitted": None,
        "lpacommentsforwarded": None,
        "statementofcommongrounddue": None,
        "statementofcommongroundreceived": None,
        "appellantcommentssubmitted": None,
        "appellantcommentsforwarded": None,
        "thirdpartyrepsdue": None,
        "thirdpartyrepsforwarded": None,
        "sitenoticesent": None,
        "proofsdue": None,
        "lpaproofssubmitted": None,
        "lpaproofsforwarded": None,
        "appellantsproofssubmitted": None,
        "appellantsproofsforwarded": None,
        "areaofsiteinhectares": None,
        "dateofdecisionifissued": None,
        "typeofapplication": None,
        "appealsourceindicator": None,
        "developmentorallegation": None,
        "siteaddress1": None,
        "siteaddress2": None,
        "sitetown": None,
        "sitecounty": None,
        "sitecountry": None,
        "sitepostcode": None,
        "abeyanceenddate": None,
        "amountdue": None,
        "name": None,
        "sitegreenbelt": None,
        "lpaapplicationreference": None,
        "lpaapplicationdate": None,
        "datenotrecoveredorderecovered": None,
        "daterecovered": None,
        "callindate": None,
        "englandwalesindicator": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def horizon_appeals_additional_data_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("appealrefnumber", StringType(), True),
            StructField("lastmodified", StringType(), True),
            StructField("decisiondate", StringType(), True),
            StructField("decisionsubmitdate", StringType(), True),
            StructField("bespokeindicator", StringType(), True),
            StructField("bespoketargetdate", StringType(), True),
            StructField("processingstate", StringType(), True),
            StructField("linkstatus", StringType(), True),
            StructField("casecloseddate", StringType(), True),
            StructField("appellant", StringType(), True),
            StructField("pcsappealwithdrawndate", StringType(), True),
            StructField("numberofresidences", StringType(), True),
            StructField("appealsdocumentscomplete", StringType(), True),
            StructField("questionnairereceived", StringType(), True),
            StructField("lpaconditionsreceived", StringType(), True),
            StructField("lpaconditionsforwarded", StringType(), True),
            StructField("statementsdue", StringType(), True),
            StructField("lpastatementsubmitted", StringType(), True),
            StructField("lpastatementforwarded", StringType(), True),
            StructField("appellantstatementsubmitted", StringType(), True),
            StructField("appellantstatementforwarded", StringType(), True),
            StructField("finalcommentsdue", StringType(), True),
            StructField("lpacommentssubmitted", StringType(), True),
            StructField("lpacommentsforwarded", StringType(), True),
            StructField("statementofcommongrounddue", StringType(), True),
            StructField("statementofcommongroundreceived", StringType(), True),
            StructField("appellantcommentssubmitted", StringType(), True),
            StructField("appellantcommentsforwarded", StringType(), True),
            StructField("thirdpartyrepsdue", StringType(), True),
            StructField("thirdpartyrepsforwarded", StringType(), True),
            StructField("sitenoticesent", StringType(), True),
            StructField("proofsdue", StringType(), True),
            StructField("lpaproofssubmitted", StringType(), True),
            StructField("lpaproofsforwarded", StringType(), True),
            StructField("appellantsproofssubmitted", StringType(), True),
            StructField("appellantsproofsforwarded", StringType(), True),
            StructField("areaofsiteinhectares", StringType(), True),
            StructField("dateofdecisionifissued", StringType(), True),
            StructField("typeofapplication", StringType(), True),
            StructField("appealsourceindicator", StringType(), True),
            StructField("developmentorallegation", StringType(), True),
            StructField("siteaddress1", StringType(), True),
            StructField("siteaddress2", StringType(), True),
            StructField("sitetown", StringType(), True),
            StructField("sitecounty", StringType(), True),
            StructField("sitecountry", StringType(), True),
            StructField("sitepostcode", StringType(), True),
            StructField("abeyanceenddate", StringType(), True),
            StructField("amountdue", StringType(), True),
            StructField("name", StringType(), True),
            StructField("sitegreenbelt", StringType(), True),
            StructField("lpaapplicationreference", StringType(), True),
            StructField("lpaapplicationdate", StringType(), True),
            StructField("datenotrecoveredorderecovered", StringType(), True),
            StructField("daterecovered", StringType(), True),
            StructField("callindate", StringType(), True),
            StructField("englandwalesindicator", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def bis_case_site_category_additional_str_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "AppealRefNumber": None,
        "AgriculturalHolding": None,
        "DevelopmentAffectSettingOfListedBuilding": None,
        "SiteGridReferenceEasting": None,
        "SiteGridReferenceNorthing": None,
        "HistoricBuildingGrantMade": None,
        "InCARelatesToCA": None,
        "InspectorNeedToEnterSite": None,
        "IsFloodingAnIssue": None,
        "IsTheSiteWithinAnAONB": None,
        "SiteWithinAGreenBelt": None,
        "SiteWithinSSSI": None,
        "ingested_by_process_name": "py_horizon_raw_to_std",
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": "py_horizon_raw_to_std",
        "entity_name": "BIS_CaseSiteCategoryAdditionalStr",
        "file_id": None,
    }
    return base | overrides


def bis_case_site_category_additional_str_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("AppealRefNumber", StringType(), True),
            StructField("AgriculturalHolding", StringType(), True),
            StructField("DevelopmentAffectSettingOfListedBuilding", StringType(), True),
            StructField("SiteGridReferenceEasting", StringType(), True),
            StructField("SiteGridReferenceNorthing", StringType(), True),
            StructField("HistoricBuildingGrantMade", StringType(), True),
            StructField("InCARelatesToCA", StringType(), True),
            StructField("InspectorNeedToEnterSite", StringType(), True),
            StructField("IsFloodingAnIssue", StringType(), True),
            StructField("IsTheSiteWithinAnAONB", StringType(), True),
            StructField("SiteWithinAGreenBelt", StringType(), True),
            StructField("SiteWithinSSSI", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def horizon_inspector_cases_row(**overrides):
    return {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "appealrefnumber": None,
        "casereference": None,
        "typeofinvolvement": "Inspector",
        "inspectorname": None,
        "contactid": None,
        "parentkeyid": None,
        "postname": None,
        "firstname": None,
        "lastname": None,
        "email": None,
        "ingested_by_process_name": "py_horizon_raw_to_std",
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": "py_horizon_raw_to_std",
        "entity_name": "InspectorCases",
        "file_id": None,
    }


def horizon_inspector_cases_schema():
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("appealrefnumber", StringType(), True),
            StructField("casereference", StringType(), True),
            StructField("typeofinvolvement", StringType(), True),
            StructField("inspectorname", StringType(), True),
            StructField("contactid", StringType(), True),
            StructField("parentkeyid", StringType(), True),
            StructField("postname", StringType(), True),
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("email", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def appeal_has_standardised_row(**overrides):
    base = {
        "caseReference": "1",
        "caseId": "1",
        "caseType": "w",
        "caseStatus": None,
        "applicationReference": None,
        "typeOfPlanningApplication": None,
        "applicationDate": None,
        "applicationDecisionDate": None,
        "caseDecisionOutcome": None,
        "jurisdiction": None,
        "redeterminedIndicator": None,
        "lpaCode": None,
        "linkedCaseStatus": None,
        "leadCaseReference": "10",
        "siteAddressTown": "townA",
        "siteAddressPostcode": "postcodeA",
        "siteAddressLine2": None,
        "siteAddressLine1": None,
        "siteAddressCounty": "townA",
        "isGreenBelt": None,
        "inConservationArea": None,
        "siteGridReferenceNorthing": None,
        "siteGridReferenceEasting": None,
        "isAonbNationalLandscape": None,
        "caseValidationOutcome": "Valid",
        "caseValidationDate": None,
        "dateCostsReportDespatched": None,
        "lpaQuestionnaireSubmittedDate": None,
        "lpaQuestionnaireDueDate": None,
        "originalCaseDecisionDate": None,
        "dateRecovered": None,
        "dateNotRecoveredOrDerecovered": None,
        "caseWithdrawnDate": None,
        "caseStartedDate": None,
        "caseDecisionOutcomeDate": None,
        "caseCreatedDate": None,
        "caseCompletedDate": None,
        "siteAreaSquareMetres": None,
        "floorSpaceSquareMetres": None,
        "originalDevelopmentDescription": None,
        "importantinformation": "infoA",
        "lpaProcedurePreference": None,
        "caseProcedure": None,
        "inspectorId": None,
        "allocationLevel": "A",
        "allocationBand": "bandA",
        "caseSpecialisms": "Specialism A",
        "caseUpdatedDate": datetime(2025, 1, 1, 0, 0),
        "advertAttributeKey": None,
        "advertType": "adTypeA",
        "advertSetRowNumber": 1,
        "publicSafetyLpaIndicator": None,
        "amenityLpaIndicator": None,
        "publicSafetyGroundOutcome": None,
        "amenityGroundOutcome": None,
        "advertInPosition": None,
        "siteOnHighwayLand": None,
    }
    return base | overrides


def appeal_has_standardised_schema():
    return StructType(
        [
            StructField("caseReference", StringType(), True),
            StructField("caseId", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("caseStatus", StringType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("typeOfPlanningApplication", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecisionDate", StringType(), True),
            StructField("caseDecisionOutcome", StringType(), True),
            StructField("jurisdiction", StringType(), True),
            StructField("redeterminedIndicator", StringType(), True),
            StructField("lpaCode", StringType(), True),
            StructField("linkedCaseStatus", StringType(), True),
            StructField("leadCaseReference", StringType(), True),
            StructField("siteAddressTown", StringType(), True),
            StructField("siteAddressPostcode", StringType(), True),
            StructField("siteAddressLine2", StringType(), True),
            StructField("siteAddressLine1", StringType(), True),
            StructField("siteAddressCounty", StringType(), True),
            StructField("isGreenBelt", StringType(), True),
            StructField("inConservationArea", StringType(), True),
            StructField("siteGridReferenceNorthing", StringType(), True),
            StructField("siteGridReferenceEasting", StringType(), True),
            StructField("isAonbNationalLandscape", StringType(), True),
            StructField("caseValidationOutcome", StringType(), True),
            StructField("caseValidationDate", StringType(), True),
            StructField("dateCostsReportDespatched", StringType(), True),
            StructField("lpaQuestionnaireSubmittedDate", StringType(), True),
            StructField("lpaQuestionnaireDueDate", StringType(), True),
            StructField("originalCaseDecisionDate", StringType(), True),
            StructField("dateRecovered", StringType(), True),
            StructField("dateNotRecoveredOrDerecovered", StringType(), True),
            StructField("caseWithdrawnDate", StringType(), True),
            StructField("caseStartedDate", StringType(), True),
            StructField("caseDecisionOutcomeDate", StringType(), True),
            StructField("caseCreatedDate", StringType(), True),
            StructField("caseCompletedDate", StringType(), True),
            StructField("siteAreaSquareMetres", DoubleType(), True),
            StructField("floorSpaceSquareMetres", StringType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("importantinformation", StringType(), True),
            StructField("lpaProcedurePreference", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("inspectorId", StringType(), True),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", StringType(), True),
            StructField("caseSpecialisms", StringType(), True),
            StructField("caseUpdatedDate", TimestampType(), True),
            StructField("advertAttributeKey", StringType(), True),
            StructField("advertType", StringType(), True),
            StructField("advertSetRowNumber", IntegerType(), True),
            StructField("publicSafetyLpaIndicator", StringType(), True),
            StructField("amenityLpaIndicator", StringType(), True),
            StructField("publicSafetyGroundOutcome", StringType(), True),
            StructField("amenityGroundOutcome", StringType(), True),
            StructField("advertInPosition", StringType(), True),
            StructField("siteOnHighwayLand", StringType(), True),
        ]
    )


class TestRefAppealHasStandardisationProcess(ETLTestCase):
    def assert_standardisation_successful(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_cases_has = spark.createDataFrame(
            (
                horizon_cases_has_row(
                    CaseNodeId="1",
                    subType="1",
                    Abbreviation="w",
                    caseReference="1",
                    caseUniqueId="1",
                ),
                horizon_cases_has_row(
                    CaseNodeId="2",
                    subType="2",
                    Abbreviation="w",
                    caseReference="2",
                    caseUniqueId="2",
                ),
                horizon_cases_has_row(
                    CaseNodeId="3",
                    subType="3",
                    Abbreviation="w",
                    caseReference="3",
                    caseUniqueId="3",
                ),
                horizon_cases_has_row(
                    CaseNodeId="4",
                    subType="4",
                    Abbreviation="w",
                    caseReference="4",
                    caseUniqueId="4",
                ),
                horizon_cases_has_row(
                    CaseNodeId="5",
                    subType="5",
                    Abbreviation="w",
                    caseReference="5",
                    caseUniqueId="5",
                ),
                horizon_cases_has_row(
                    CaseNodeId="6",
                    subType="6",
                    Abbreviation="w",
                    caseReference="6",
                    caseUniqueId="6",
                ),
                horizon_cases_has_row(
                    CaseNodeId="7",
                    subType="7",
                    Abbreviation="w",
                    caseReference="7",
                    caseUniqueId="7",
                ),
                horizon_cases_has_row(
                    CaseNodeId="8",
                    subType="8",
                    Abbreviation="w",
                    caseReference="8",
                    caseUniqueId="8",
                ),
                horizon_cases_has_row(
                    CaseNodeId="9",
                    subType="9",
                    Abbreviation="w",
                    caseReference="9",
                    caseUniqueId="9",
                ),
                horizon_cases_has_row(
                    CaseNodeId="10",
                    subType="10",
                    Abbreviation="w",
                    caseReference="10",
                    caseUniqueId="10",
                ),
            ),
            schema=horizon_cases_has_schema(),
        )
        horizon_cases_has_table = f"{test_case}_horizon_cases_has"
        self.write_existing_table(
            spark,
            horizon_cases_has,
            horizon_cases_has_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_cases_has_table,
            "overwrite",
        )
        # cs
        cases_specialisms = spark.createDataFrame(
            (
                cases_specialisms_row(casereference="1", casespecialism="Specialism A"),
                cases_specialisms_row(casereference="2", casespecialism="Specialism B"),
                cases_specialisms_row(casereference="3", casespecialism="Specialism C"),
                cases_specialisms_row(casereference="4", casespecialism="Specialism D"),
                cases_specialisms_row(casereference="5", casespecialism="Specialism E"),
                cases_specialisms_row(casereference="6", casespecialism="Specialism F"),
                cases_specialisms_row(casereference="7", casespecialism="Specialism G"),
                cases_specialisms_row(casereference="8", casespecialism="Specialism H"),
                cases_specialisms_row(casereference="9", casespecialism="Specialism I"),
                cases_specialisms_row(casereference="10", casespecialism="Specialism J"),
            ),
            schema=cases_specialisms_schema(),
        )
        cases_specialisms_table = f"{test_case}_cases_specialisms"
        self.write_existing_table(
            spark,
            cases_specialisms,
            cases_specialisms_table,
            "odw_standardised_db",
            "odw-standardised",
            cases_specialisms_table,
            "overwrite",
        )
        # cd
        vw_case_dates = spark.createDataFrame(
            (
                vw_cases_dates_row(casenodeid="1"),
                vw_cases_dates_row(casenodeid="2"),
                vw_cases_dates_row(casenodeid="3"),
                vw_cases_dates_row(casenodeid="4"),
                vw_cases_dates_row(casenodeid="5"),
                vw_cases_dates_row(casenodeid="6"),
                vw_cases_dates_row(casenodeid="7"),
                vw_cases_dates_row(casenodeid="8"),
                vw_cases_dates_row(casenodeid="9"),
                vw_cases_dates_row(casenodeid="10"),
            ),
            schema=vw_cases_dates_schema(),
        )
        vw_case_dates_table = f"{test_case}_vw_case_dates"
        self.write_existing_table(
            spark,
            vw_case_dates,
            vw_case_dates_table,
            "odw_standardised_db",
            "odw-standardised",
            vw_case_dates_table,
            "overwrite",
        )
        # cdd
        casedocumentdatesdates = spark.createDataFrame(
            (
                case_document_dates_dates_row(casenodeid="1", appealrefnumber="1"),
                case_document_dates_dates_row(casenodeid="2", appealrefnumber="2"),
                case_document_dates_dates_row(casenodeid="3", appealrefnumber="3"),
                case_document_dates_dates_row(casenodeid="4", appealrefnumber="4"),
                case_document_dates_dates_row(casenodeid="5", appealrefnumber="5"),
                case_document_dates_dates_row(casenodeid="6", appealrefnumber="6"),
                case_document_dates_dates_row(casenodeid="7", appealrefnumber="7"),
                case_document_dates_dates_row(casenodeid="8", appealrefnumber="8"),
                case_document_dates_dates_row(casenodeid="9", appealrefnumber="9"),
                case_document_dates_dates_row(casenodeid="10", appealrefnumber="10"),
            ),
            schema=case_document_dates_dates_schema(),
        )
        casedocumentdatesdates_table = f"{test_case}_casedocumentdatesdates"
        self.write_existing_table(
            spark,
            casedocumentdatesdates,
            casedocumentdatesdates_table,
            "odw_standardised_db",
            "odw-standardised",
            casedocumentdatesdates_table,
            "overwrite",
        )
        # css
        casesitestrings = spark.createDataFrame(
            (
                case_site_strings_row(
                    casenodeid="1",
                    town="townA",
                    county="townA",
                    postcode="postcodeA",
                    country="countryA",
                ),
                case_site_strings_row(
                    casenodeid="2",
                    town="townB",
                    county="townB",
                    postcode="postcodeB",
                    country="countryB",
                ),
                case_site_strings_row(
                    casenodeid="3",
                    town="townC",
                    county="townC",
                    postcode="postcodeC",
                    country="countryC",
                ),
                case_site_strings_row(
                    casenodeid="4",
                    town="townD",
                    county="townD",
                    postcode="postcodeD",
                    country="countryD",
                ),
                case_site_strings_row(
                    casenodeid="5",
                    town="townE",
                    county="townE",
                    postcode="postcodeE",
                    country="countryE",
                ),
                case_site_strings_row(
                    casenodeid="6",
                    town="townF",
                    county="townF",
                    postcode="postcodeF",
                    country="countryF",
                ),
                case_site_strings_row(
                    casenodeid="7",
                    town="townG",
                    county="townG",
                    postcode="postcodeG",
                    country="countryG",
                ),
                case_site_strings_row(
                    casenodeid="8",
                    town="townH",
                    county="townH",
                    postcode="postcodeH",
                    country="countryH",
                ),
                case_site_strings_row(
                    casenodeid="9",
                    town="townI",
                    county="townI",
                    postcode="postcodeI",
                    country="countryI",
                ),
                case_site_strings_row(
                    casenodeid="10",
                    town="townJ",
                    county="townJ",
                    postcode="postcodeJ",
                    country="countrJ",
                ),
            ),
            schema=case_site_strings_schema(),
        )
        casesitestrings_table = f"{test_case}_casesitestrings"
        self.write_existing_table(
            spark,
            casesitestrings,
            casesitestrings_table,
            "odw_standardised_db",
            "odw-standardised",
            casesitestrings_table,
            "overwrite",
        )
        # tp
        typeofprocedure = spark.createDataFrame(
            (
                type_of_procedure_row(id="1", name="nameA", proccode="codeA"),
                type_of_procedure_row(id="2", name="nameB", proccode="codeB"),
                type_of_procedure_row(id="3", name="nameC", proccode="codeC"),
                type_of_procedure_row(id="4", name="nameD", proccode="codeD"),
                type_of_procedure_row(id="5", name="nameE", proccode="codeE"),
                type_of_procedure_row(id="6", name="nameF", proccode="codeF"),
                type_of_procedure_row(id="7", name="nameG", proccode="codeG"),
                type_of_procedure_row(id="8", name="nameH", proccode="codeH"),
                type_of_procedure_row(id="9", name="nameI", proccode="codeI"),
                type_of_procedure_row(id="10", name="nameJ", proccode="codeJ"),
            ),
            schema=type_of_procedure_schema(),
        )
        typeofprocedure_table = f"{test_case}_typeofprocedure"
        self.write_existing_table(
            spark,
            typeofprocedure,
            typeofprocedure_table,
            "odw_standardised_db",
            "odw-standardised",
            typeofprocedure_table,
            "overwrite",
        )
        # aad_old
        vw_addadditionaldata = spark.createDataFrame(
            (
                vw_additional_data_row(appealrefnumber="1", level="A"),
                vw_additional_data_row(appealrefnumber="2", level="B"),
                vw_additional_data_row(appealrefnumber="3", level="C"),
                vw_additional_data_row(appealrefnumber="4", level="D"),
                vw_additional_data_row(appealrefnumber="5", level="E"),
                vw_additional_data_row(appealrefnumber="6", level="F"),
                vw_additional_data_row(appealrefnumber="7", level="G"),
                vw_additional_data_row(appealrefnumber="8", level="H"),
                vw_additional_data_row(appealrefnumber="9", level="I"),
                vw_additional_data_row(appealrefnumber="10", level="J"),
            ),
            schema=vw_additional_data_schema(),
        )
        vw_addadditionaldata_table = f"{test_case}_vw_addadditionaldata"
        self.write_existing_table(
            spark,
            vw_addadditionaldata,
            vw_addadditionaldata_table,
            "odw_standardised_db",
            "odw-standardised",
            vw_addadditionaldata_table,
            "overwrite",
        )
        # af
        vw_additionalfields = spark.createDataFrame(
            (
                vw_additional_fields_row(
                    appealrefnumber="1",
                    casereference="1",
                    importantinformation="infoA",
                ),
                vw_additional_fields_row(
                    appealrefnumber="2",
                    casereference="2",
                    importantinformation="infoB",
                ),
                vw_additional_fields_row(
                    appealrefnumber="3",
                    casereference="3",
                    importantinformation="infoC",
                ),
                vw_additional_fields_row(
                    appealrefnumber="4",
                    casereference="4",
                    importantinformation="infoD",
                ),
                vw_additional_fields_row(
                    appealrefnumber="5",
                    casereference="5",
                    importantinformation="infoE",
                ),
                vw_additional_fields_row(
                    appealrefnumber="6",
                    casereference="6",
                    importantinformation="infoF",
                ),
                vw_additional_fields_row(
                    appealrefnumber="7",
                    casereference="7",
                    importantinformation="infoG",
                ),
                vw_additional_fields_row(
                    appealrefnumber="8",
                    casereference="8",
                    importantinformation="infoH",
                ),
                vw_additional_fields_row(
                    appealrefnumber="9",
                    casereference="9",
                    importantinformation="infoI",
                ),
                vw_additional_fields_row(
                    appealrefnumber="10",
                    casereference="10",
                    importantinformation="infoJ",
                ),
            ),
            schema=vw_additional_fields_schema(),
        )
        vw_additionalfields_table = f"{test_case}_vw_additionalfields"
        self.write_existing_table(
            spark,
            vw_additionalfields,
            vw_additionalfields_table,
            "odw_standardised_db",
            "odw-standardised",
            vw_additionalfields_table,
            "overwrite",
        )
        # haa
        horizon_advert_attributes = spark.createDataFrame(
            (
                horizon_advert_attributes_row(caseNodeId=1, caseUniqueId=1, advertType="adTypeA"),
                horizon_advert_attributes_row(caseNodeId=2, caseUniqueId=2, advertType="adTypeB"),
                horizon_advert_attributes_row(caseNodeId=3, caseUniqueId=3, advertType="adTypeC"),
                horizon_advert_attributes_row(caseNodeId=4, caseUniqueId=4, advertType="adTypeD"),
                horizon_advert_attributes_row(caseNodeId=5, caseUniqueId=5, advertType="adTypeE"),
                horizon_advert_attributes_row(caseNodeId=6, caseUniqueId=6, advertType="adTypeF"),
                horizon_advert_attributes_row(caseNodeId=7, caseUniqueId=7, advertType="adTypeG"),
                horizon_advert_attributes_row(caseNodeId=8, caseUniqueId=8, advertType="adTypeH"),
                horizon_advert_attributes_row(caseNodeId=9, caseUniqueId=9, advertType="adTypeI"),
                horizon_advert_attributes_row(caseNodeId=10, caseUniqueId=10, advertType="adTypeJ"),
            ),
            schema=horizon_advert_attributes_schema(),
        )
        horizon_advert_attributes_table = f"{test_case}_horizon_advert_attributes"
        self.write_existing_table(
            spark,
            horizon_advert_attributes,
            horizon_advert_attributes_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_advert_attributes_table,
            "overwrite",
        )
        # ctl
        typeoflevel = spark.createDataFrame(
            (
                type_of_level_row(id=1, name="A", band="bandA"),
                type_of_level_row(id=2, name="B", band="bandB"),
                type_of_level_row(id=3, name="C", band="bandC"),
                type_of_level_row(id=4, name="D", band="bandD"),
                type_of_level_row(id=5, name="E", band="bandE"),
                type_of_level_row(id=6, name="F", band="bandF"),
                type_of_level_row(id=7, name="G", band="bandG"),
                type_of_level_row(id=8, name="H", band="bandH"),
                type_of_level_row(id=9, name="I", band="bandI"),
                type_of_level_row(id=10, name="J", band="bandJ"),
            ),
            schema=type_of_level_schema(),
        )
        typeoflevel_table = f"{test_case}_TypeOfLevel"
        self.write_existing_table(
            spark,
            typeoflevel,
            typeoflevel_table,
            "odw_standardised_db",
            "odw-standardised",
            typeoflevel_table,
            "overwrite",
        )
        # scd
        horizon_specialist_case_dates = spark.createDataFrame(
            (
                horizon_specialist_case_dates_row(appealrefnumber="1"),
                horizon_specialist_case_dates_row(appealrefnumber="2"),
                horizon_specialist_case_dates_row(appealrefnumber="3"),
                horizon_specialist_case_dates_row(appealrefnumber="4"),
                horizon_specialist_case_dates_row(appealrefnumber="5"),
                horizon_specialist_case_dates_row(appealrefnumber="6"),
                horizon_specialist_case_dates_row(appealrefnumber="7"),
                horizon_specialist_case_dates_row(appealrefnumber="8"),
                horizon_specialist_case_dates_row(appealrefnumber="9"),
                horizon_specialist_case_dates_row(appealrefnumber="10"),
            ),
            schema=horizon_specialist_case_dates_schema(),
        )
        horizon_specialist_case_dates_table = f"{test_case}_horizon_specialist_case_dates"
        self.write_existing_table(
            spark,
            horizon_specialist_case_dates,
            horizon_specialist_case_dates_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_specialist_case_dates_table,
            "overwrite",
        )
        # pas
        planningappstrings = spark.createDataFrame(
            (
                planning_app_strings_row(casenodeid="1", planningapplicationtype="appTypeA"),
                planning_app_strings_row(casenodeid="2", planningapplicationtype="appTypeB"),
                planning_app_strings_row(casenodeid="3", planningapplicationtype="appTypeC"),
                planning_app_strings_row(casenodeid="4", planningapplicationtype="appTypeD"),
                planning_app_strings_row(casenodeid="5", planningapplicationtype="appTypeE"),
                planning_app_strings_row(casenodeid="6", planningapplicationtype="appTypeF"),
                planning_app_strings_row(casenodeid="7", planningapplicationtype="appTypeG"),
                planning_app_strings_row(casenodeid="8", planningapplicationtype="appTypeH"),
                planning_app_strings_row(casenodeid="9", planningapplicationtype="appTypeI"),
                planning_app_strings_row(casenodeid="10", planningapplicationtype="appTypeJ"),
            ),
            schema=planning_app_strings_schema(),
        )
        planningappstrings_table = f"{test_case}_PlanningAppStrings"
        self.write_existing_table(
            spark,
            planningappstrings,
            planningappstrings_table,
            "odw_standardised_db",
            "odw-standardised",
            planningappstrings_table,
            "overwrite",
        )
        # pad
        planningappdates = spark.createDataFrame(
            (
                planning_app_dates_row(casenodeid="1"),
                planning_app_dates_row(casenodeid="2"),
                planning_app_dates_row(casenodeid="3"),
                planning_app_dates_row(casenodeid="4"),
                planning_app_dates_row(casenodeid="5"),
                planning_app_dates_row(casenodeid="6"),
                planning_app_dates_row(casenodeid="7"),
                planning_app_dates_row(casenodeid="8"),
                planning_app_dates_row(casenodeid="9"),
                planning_app_dates_row(casenodeid="10"),
            ),
            schema=planning_app_dates_schema(),
        )
        planningappdates_table = f"{test_case}_PlanningAppDates"
        self.write_existing_table(
            spark,
            planningappdates,
            planningappdates_table,
            "odw_standardised_db",
            "odw-standardised",
            planningappdates_table,
            "overwrite",
        )
        # lc
        bis_leadcase = spark.createDataFrame(
            (
                bis_lead_case_row(casenodeid="1", leadcasenodeid="10"),
                bis_lead_case_row(casenodeid="2", leadcasenodeid="9"),
                bis_lead_case_row(casenodeid="3", leadcasenodeid="8"),
                bis_lead_case_row(casenodeid="4", leadcasenodeid="7"),
                bis_lead_case_row(casenodeid="5", leadcasenodeid="6"),
                bis_lead_case_row(casenodeid="6", leadcasenodeid="5"),
                bis_lead_case_row(casenodeid="7", leadcasenodeid="4"),
                bis_lead_case_row(casenodeid="8", leadcasenodeid="3"),
                bis_lead_case_row(casenodeid="9", leadcasenodeid="2"),
                bis_lead_case_row(casenodeid="10", leadcasenodeid="1"),
            ),
            schema=bis_lead_case_schema(),
        )
        bis_leadcase_table = f"{test_case}_BIS_LeadCase"
        self.write_existing_table(
            spark,
            bis_leadcase,
            bis_leadcase_table,
            "odw_standardised_db",
            "odw-standardised",
            bis_leadcase_table,
            "overwrite",
        )
        # cs2
        casestrings = spark.createDataFrame(
            (
                case_strings_row(casenodeid="1", proceduretype="procTypeA"),
                case_strings_row(casenodeid="2", proceduretype="procTypeB"),
                case_strings_row(casenodeid="3", proceduretype="procTypeC"),
                case_strings_row(casenodeid="4", proceduretype="procTypeD"),
                case_strings_row(casenodeid="5", proceduretype="procTypeE"),
                case_strings_row(casenodeid="6", proceduretype="procTypeF"),
                case_strings_row(casenodeid="7", proceduretype="procTypeG"),
                case_strings_row(casenodeid="8", proceduretype="procTypeH"),
                case_strings_row(casenodeid="9", proceduretype="procTypeI"),
                case_strings_row(casenodeid="10", proceduretype="procTypeJ"),
            ),
            schema=case_strings_schema(),
        )
        casestrings_table = f"{test_case}_CaseStrings"
        self.write_existing_table(
            spark,
            casestrings,
            casestrings_table,
            "odw_standardised_db",
            "odw-standardised",
            casestrings_table,
            "overwrite",
        )
        # ci
        horizon_case_info = spark.createDataFrame(
            (
                horizon_case_info_row(appealrefnumber="1", validity="Valid"),
                horizon_case_info_row(appealrefnumber="2", validity="Valid"),
                horizon_case_info_row(appealrefnumber="3", validity="Valid"),
                horizon_case_info_row(appealrefnumber="4", validity="Valid"),
                horizon_case_info_row(appealrefnumber="5", validity="Valid"),
                horizon_case_info_row(appealrefnumber="6", validity="Valid"),
                horizon_case_info_row(appealrefnumber="7", validity="Valid"),
                horizon_case_info_row(appealrefnumber="8", validity="Valid"),
                horizon_case_info_row(appealrefnumber="9", validity="Valid"),
                horizon_case_info_row(appealrefnumber="10", validity="Valid"),
            ),
            schema=horizon_case_info_schema(),
        )
        horizon_case_info_table = f"{test_case}_horizon_case_info"
        self.write_existing_table(
            spark,
            horizon_case_info,
            horizon_case_info_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_case_info_table,
            "overwrite",
        )
        # cdh
        horizon_case_dates = spark.createDataFrame(
            (
                horizon_case_dates_row(appealrefnumber="1"),
                horizon_case_dates_row(appealrefnumber="2"),
                horizon_case_dates_row(appealrefnumber="3"),
                horizon_case_dates_row(appealrefnumber="4"),
                horizon_case_dates_row(appealrefnumber="5"),
                horizon_case_dates_row(appealrefnumber="6"),
                horizon_case_dates_row(appealrefnumber="7"),
                horizon_case_dates_row(appealrefnumber="8"),
                horizon_case_dates_row(appealrefnumber="9"),
                horizon_case_dates_row(appealrefnumber="10"),
            ),
            schema=horizon_case_dates_schema(),
        )
        horizon_case_dates_table = f"{test_case}_horizon_case_dates"
        self.write_existing_table(
            spark,
            horizon_case_dates,
            horizon_case_dates_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_case_dates_table,
            "overwrite",
        )
        # aad
        horizon_appeals_additional_data = spark.createDataFrame(
            (
                horizon_appeals_additional_data_row(appealrefnumber="1", processingstate="Complete", appellant="Alice"),
                horizon_appeals_additional_data_row(appealrefnumber="2", processingstate="Complete", appellant="Bob"),
                horizon_appeals_additional_data_row(appealrefnumber="3", processingstate="Complete", appellant="Charlie"),
                horizon_appeals_additional_data_row(appealrefnumber="4", processingstate="Complete", appellant="Dave"),
                horizon_appeals_additional_data_row(appealrefnumber="5", processingstate="Complete", appellant="Emily"),
                horizon_appeals_additional_data_row(appealrefnumber="6", processingstate="Complete", appellant="Frank"),
                horizon_appeals_additional_data_row(appealrefnumber="7", processingstate="Complete", appellant="Garry"),
                horizon_appeals_additional_data_row(appealrefnumber="8", processingstate="Complete", appellant="Harry"),
                horizon_appeals_additional_data_row(appealrefnumber="9", processingstate="Complete", appellant="Ivy"),
                horizon_appeals_additional_data_row(appealrefnumber="10", processingstate="Complete", appellant="Jim"),
            ),
            schema=horizon_appeals_additional_data_schema(),
        )
        horizon_appeals_additional_data_table = f"{test_case}_horizon_appeals_additional_data"
        self.write_existing_table(
            spark,
            horizon_appeals_additional_data,
            horizon_appeals_additional_data_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_appeals_additional_data_table,
            "overwrite",
        )
        bis_case_site_category_additional_str = spark.createDataFrame(
            (bis_case_site_category_additional_str_row(),), schema=bis_case_site_category_additional_str_schema()
        )
        bis_case_site_category_additional_str_table = f"{test_case}_bis_case_site_category_additional_str_table"
        self.write_existing_table(
            spark,
            bis_case_site_category_additional_str,
            bis_case_site_category_additional_str_table,
            "odw_standardised_db",
            "odw-standardised",
            bis_case_site_category_additional_str_table,
            "overwrite",
        )
        horizon_inspector_cases = spark.createDataFrame((horizon_inspector_cases_row(),), schema=horizon_inspector_cases_schema())
        horizon_inspector_cases_table = f"{test_case}_horizon_inspector_cases"
        self.write_existing_table(
            spark,
            horizon_inspector_cases,
            horizon_inspector_cases_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_inspector_cases_table,
            "overwrite",
        )
        # This was generated by feeding the above test data through the original notebook
        expected_data_after_writing = spark.createDataFrame(
            (
                appeal_has_standardised_row(),
                appeal_has_standardised_row(
                    caseReference="10",
                    caseId="10",
                    leadCaseReference="1",
                    siteAddressTown="townJ",
                    siteAddressPostcode="postcodeJ",
                    siteAddressCounty="townJ",
                    importantinformation="infoJ",
                    allocationLevel="J",
                    allocationBand="bandJ",
                    caseSpecialisms="Specialism J",
                    advertType="adTypeJ",
                ),
                appeal_has_standardised_row(
                    caseReference="2",
                    caseId="2",
                    leadCaseReference="9",
                    siteAddressTown="townB",
                    siteAddressPostcode="postcodeB",
                    siteAddressCounty="townB",
                    importantinformation="infoB",
                    allocationLevel="B",
                    allocationBand="bandB",
                    caseSpecialisms="Specialism B",
                    advertType="adTypeB",
                ),
                appeal_has_standardised_row(
                    caseReference="3",
                    caseId="3",
                    leadCaseReference="8",
                    siteAddressTown="townC",
                    siteAddressPostcode="postcodeC",
                    siteAddressCounty="townC",
                    importantinformation="infoC",
                    allocationLevel="C",
                    allocationBand="bandC",
                    caseSpecialisms="Specialism C",
                    advertType="adTypeC",
                ),
                appeal_has_standardised_row(
                    caseReference="4",
                    caseId="4",
                    leadCaseReference="7",
                    siteAddressTown="townD",
                    siteAddressPostcode="postcodeD",
                    siteAddressCounty="townD",
                    importantinformation="infoD",
                    allocationLevel="D",
                    allocationBand="bandD",
                    caseSpecialisms="Specialism D",
                    advertType="adTypeD",
                ),
                appeal_has_standardised_row(
                    caseReference="5",
                    caseId="5",
                    leadCaseReference="6",
                    siteAddressTown="townE",
                    siteAddressPostcode="postcodeE",
                    siteAddressCounty="townE",
                    importantinformation="infoE",
                    allocationLevel="E",
                    allocationBand="bandE",
                    caseSpecialisms="Specialism E",
                    advertType="adTypeE",
                ),
                appeal_has_standardised_row(
                    caseReference="6",
                    caseId="6",
                    leadCaseReference="5",
                    siteAddressTown="townF",
                    siteAddressPostcode="postcodeF",
                    siteAddressCounty="townF",
                    importantinformation="infoF",
                    allocationLevel="F",
                    allocationBand="bandF",
                    caseSpecialisms="Specialism F",
                    advertType="adTypeF",
                ),
                appeal_has_standardised_row(
                    caseReference="7",
                    caseId="7",
                    leadCaseReference="4",
                    siteAddressTown="townG",
                    siteAddressPostcode="postcodeG",
                    siteAddressCounty="townG",
                    importantinformation="infoG",
                    allocationLevel="G",
                    allocationBand="bandG",
                    caseSpecialisms="Specialism G",
                    advertType="adTypeG",
                ),
                appeal_has_standardised_row(
                    caseReference="8",
                    caseId="8",
                    leadCaseReference="3",
                    siteAddressTown="townH",
                    siteAddressPostcode="postcodeH",
                    siteAddressCounty="townH",
                    importantinformation="infoH",
                    allocationLevel="H",
                    allocationBand="bandH",
                    caseSpecialisms="Specialism H",
                    advertType="adTypeH",
                ),
                appeal_has_standardised_row(
                    caseReference="9",
                    caseId="9",
                    leadCaseReference="2",
                    siteAddressTown="townI",
                    siteAddressPostcode="postcodeI",
                    siteAddressCounty="townI",
                    importantinformation="infoI",
                    allocationLevel="I",
                    allocationBand="bandI",
                    caseSpecialisms="Specialism I",
                    advertType="adTypeI",
                ),
            ),
            schema=appeal_has_standardised_schema(),
        )
        output_table = f"{test_case}_horizon_appeal_has"
        property_override_map = {
            "STANDARDISED_HORIZON_CASES_HAS": horizon_cases_has,
            "STANDARDISED_CASES_SPECIALISMS": cases_specialisms_table,
            "STANDARDISED_VW_CASE_DATES": vw_case_dates_table,
            "STANDARDISED_CASE_DOCUMENT_DATES_DATES": casedocumentdatesdates_table,
            "STANDARDISED_CASE_SITE_STRINGS": casesitestrings_table,
            "STANDARDISED_TYPE_OF_PROCEDURE": typeofprocedure_table,
            "STANDARDISED_ADD_ADDITIONAL_DATA": vw_addadditionaldata_table,
            "STANDARDISED_ADDITIONAL_FIELDS": vw_additionalfields_table,
            "STANDARDISED_HORIZON_ADVERT_ATTRIBUTES": horizon_advert_attributes_table,
            "STANDARDISED_TYPE_OF_LEVEL": typeoflevel_table,
            "STANDARDISED_HORIZON_SPECIALIST_CASE_DATES": horizon_specialist_case_dates_table,
            "STANDARDISED_PLANNING_APP_STRINGS": planningappstrings_table,
            "STANDARDISED_PLANNING_APP_DATES": planningappdates_table,
            "STANDARDISED_LEAD_CASE": bis_leadcase_table,
            "STANDARDISED_CASE_STRINGS": casestrings_table,
            "STANDARDISED_HORIZON_CASE_INFO": horizon_case_info_table,
            "STANDARDISED_HORIZON_CASE_DATES": horizon_case_dates_table,
            "STANDARDISED_HORIZON_APPEALS_ADDITIONAL_DATA": horizon_appeals_additional_data_table,
            "OUTPUT_TABLE": output_table,
        }
        with ExitStack() as stack:
            for s78_property, override_value in property_override_map.items():
                stack.enter_context(mock.patch.object(AppealHasStandardisationProcess, s78_property, override_value))
            inst = AppealHasStandardisationProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_standardised_db.{output_table}")
            assert_dataframes_equal(expected_data_after_writing, actual_table_data)

    def test__ref_appeal_has_standardisation_process__run__with_no_existing_data(self):
        """
        - Given there are existing standardised tables related to appeal has
        - When I call AppealHasStandardisationProcess.run
        - Then the appeal has table should be created in the standardised layer
        """
        self.assert_standardisation_successful("t_rahsp_r_wned")

    def test__ref_appeal_has_standardisation_process__run__with_existing_data(self):
        """
        - Given there are existing standardised tables related to appeal has and an existing standardised appeal has table
        - When I call AppealHasStandardisationProcess.run
        - Then the appeal has table should be created in the standardised layer and the old appeal has data overwritten
        """
        test_case = "t_rahsp_r_wed"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_appeal_has = spark.createDataFrame((appeal_has_standardised_row(caseReference="25"),), schema=appeal_has_standardised_schema())
        output_table = f"{test_case}_horizon_appeal_has"
        self.write_existing_table(spark, existing_appeal_has, output_table, "odw_standardised_db", "odw-standardised", output_table, "overwrite")
        self.assert_standardisation_successful("t_rahsp_r_wed")
