import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType, IntegerType, BooleanType
from odw.core.etl.transformation.standardised.appeal_s78_standardisation_process import AppealS78StandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from datetime import datetime
from contextlib import ExitStack


def generate_horizoncases_s78_row(**overrides):
    # h
    """h"""
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
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
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": "HorizonCases_s78",
        "file_id": None,
    }
    return base | overrides


def generate_horizoncases_s78_schema():
    # h
    """
    h
    """
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


def generate_cases_specialisms_row(**overrides):
    # cs
    """
    cs
    """
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


def generate_cases_specialisms_schema():
    # cs
    """
    cs
    """
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


def generate_vw_case_dates_row(**overrides):
    # cd
    """
    cd
    """
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


def generate_vw_case_dates_schema():
    # cd
    """
    cd
    """
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


def generate_casedocumentdatesdates_row(**overrides):
    # cdd
    """
    cdd
    """
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


def generate_casedocumentdatesdates_schema():
    # cdd
    """
    cdd
    """
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


def generate_casesitestrings_row(**overrides):
    # css
    """
    css
    """
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


def generate_casesitestrings_schema():
    # css
    """
    css
    """
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


def generate_typeofprocedure_row(**overrides):
    # tp
    """
    tp
    """
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


def generate_typeofprocedure_schema():
    # tp
    """
    tp
    """
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


def generate_vw_addadditionaldata_row(**overrides):
    # aad_old
    """
    aad_old
    """
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


def generate_vw_addadditionaldata_schema():
    # aad_old
    """
    aad_old
    """
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


def generate_vw_additionalfields_row(**overrides):
    # af
    """
    af
    """
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


def generate_vw_additionalfields_schema():
    # af
    """
    af
    """
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


def generate_horizon_advert_attributes_row(**overrides):
    # haa
    """
    haa
    """
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


def generate_horizon_advert_attributes_schema():
    # haa
    """
    haa
    """
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


def generate_typeoflevel_row(**overrides):
    # ctl
    """
    ctl
    """
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


def generate_typeoflevel_schema():
    # ctl
    """
    ctl
    """
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


def generate_horizon_specialist_case_dates_row(**overrides):
    # scd
    """
    scd
    """
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


def generate_horizon_specialist_case_dates_schema():
    # scd
    """
    scd
    """
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


def generate_planningappstrings_row(**overrides):
    # pas
    """
    pas
    """
    base = {}
    return base | overrides


def generate_planningappstrings_schema():
    # pas
    """
    pas
    """
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


def generate_planningappdates_row(**overrides):
    # pad
    """
    pad
    """
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


def generate_planningappdates_schema():
    # pad
    """
    pad
    """
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


def generate_bis_leadcase_row(**overrides):
    # lc
    """
    lc
    """
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


def generate_bis_leadcase_schema():
    # lc
    """
    lc
    """
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


def generate_casestrings_row(**overrides):
    # cs2
    """
    cs2
    """
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


def generate_casestrings_schema():
    # cs2
    """
    cs2
    """
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


def generate_horizon_case_info_row(**overrides):
    # ci
    """
    ci
    """
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


def generate_horizon_case_info_schema():
    # ci
    """
    ci
    """
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


def generate_horizon_case_dates_row(**overrides):
    # cdh
    """
    cdh
    """
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


def generate_horizon_case_dates_schema():
    # cdh
    """
    cdh
    """
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


def generate_horizon_appeals_additional_data_row(**overrides):
    # aad
    """
    aad
    """
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


def generate_horizon_appeals_additional_data_schema():
    # aad
    """
    aad
    """
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


def generate_horizon_appeal_grounds_row(**overrides):
    # hag
    """
    hag
    """
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "groundsKey": None,
        "caseNodeId": None,
        "setRowNumber": 1,
        "appealGroundLetter": None,
        "groundForAppealStartDate": datetime(2025, 1, 1),
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def generate_horizon_appeal_grounds_schema():
    # hag
    """
    hag
    """
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("groundsKey", StringType(), True),
            StructField("caseNodeId", IntegerType(), True),
            StructField("setRowNumber", IntegerType(), True),
            StructField("appealGroundLetter", StringType(), True),
            StructField("groundForAppealStartDate", TimestampType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
        ]
    )


def generate_horizon_notice_dates_row(**overrides):
    # hnd
    """
    hnd
    """
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "caseUniqueId": None,
        "caseNodeId": None,
        "setRowNumber": 1,
        "issueDate": datetime(2025, 1, 1),
        "effectiveDate": datetime(2025, 1, 1),
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
    }
    return base | overrides


def generate_horizon_notice_dates_schema():
    # hnd
    """
    hnd
    """
    return StructType(
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
    )


def generate_horizon_application_made_under_section_row(**overrides):
    # hmu
    """
    hmu
    """
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "caseUniqueId": None,
        "caseNodeId": None,
        "applicationMadeUnderSection": None,
        "ingested_by_process_name": None,
        "input_file": None,
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": None,
        "entity_name": None,
        "file_id": None,
        "retrospectiveapplication": None,
    }
    return base | overrides


def generate_horizon_application_made_under_section_schema():
    # hmu
    """
    hmu
    """
    return StructType(
        [
            StructField("ingested_datetime", TimestampType(), True),
            StructField("expected_from", TimestampType(), True),
            StructField("expected_to", TimestampType(), True),
            StructField("caseUniqueId", IntegerType(), True),
            StructField("caseNodeId", IntegerType(), True),
            StructField("applicationMadeUnderSection", StringType(), True),
            StructField("ingested_by_process_name", StringType(), True),
            StructField("input_file", StringType(), True),
            StructField("modified_datetime", TimestampType(), True),
            StructField("modified_by_process_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("file_id", StringType(), True),
            StructField("retrospectiveapplication", StringType(), True),
        ]
    )


def generate_expected_row(**overrides):
    base = {
        "ingested_datetime": datetime(2025, 1, 1),
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "modified_datetime": datetime(2025, 1, 1),
        "issueDateOfEnforcementNotice": datetime(2025, 1, 1),
        "effectiveDateOfEnforcementNotice": datetime(2025, 1, 1),
        "rn_final": 1,
        "rn": 1,
        "setRowNumber": 1,
        "entity_name": "HorizonCases_s78",
        "caseValidationOutcome": "Valid",
        "abbreviation": "w",
    }
    return base | overrides


def generate_expected_schema():
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
            StructField("rn", IntegerType(), False),
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
            StructField("issueDateOfEnforcementNotice", TimestampType(), True),
            StructField("effectiveDateOfEnforcementNotice", TimestampType(), True),
            StructField(
                "enforcementAppealGroundsDetails",
                ArrayType(
                    StructType(
                        [StructField("appealGroundLetter", StringType(), True), StructField("groundForAppealStartDate", TimestampType(), True)]
                    ),
                    False,
                ),
                True,
            ),
            StructField("allocationLevel", StringType(), True),
            StructField("allocationBand", StringType(), True),
            StructField("applicationReference", StringType(), True),
            StructField("typeOfPlanningApplication", StringType(), True),
            StructField("applicationDate", StringType(), True),
            StructField("applicationDecisionDate", StringType(), True),
            StructField("applicationMadeUnderActSection", StringType(), True),
            StructField("advertAttributeKey", StringType(), True),
            StructField("advertType", StringType(), True),
            StructField("setRowNumber", IntegerType(), True),
            StructField("publicSafetyLpaIndicator", StringType(), True),
            StructField("amenityLPAIndicator", StringType(), True),
            StructField("publicSafetyGroundOutcome", StringType(), True),
            StructField("amenityGroundOutcome", StringType(), True),
            StructField("advertInPosition", StringType(), True),
            StructField("siteOnHighwayLand", StringType(), True),
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
            StructField("caseValidationDate", StringType(), True),
            StructField("dateCostsReportDespatched", StringType(), True),
            StructField("originalDevelopmentDescription", StringType(), True),
            StructField("appellantCommentsSubmittedDate", StringType(), True),
            StructField("appellantStatementSubmittedDate", StringType(), True),
            StructField("lpaCommentsSubmittedDate", StringType(), True),
            StructField("lpaProofsSubmittedDate", StringType(), True),
            StructField("lpaStatementSubmittedDate", StringType(), True),
            StructField("siteNoticesSentDate", StringType(), True),
            StructField("statementDueDate", StringType(), True),
            StructField("numberOfResidencesNetChange", StringType(), True),
            StructField("caseProcedure", StringType(), True),
            StructField("rn_final", IntegerType(), False),
            StructField("preserveGrantLoan", BooleanType(), True),
            StructField("consultHistoricEngland", BooleanType(), True),
        ]
    )


class TestAppealS78StandardisationProcess(ETLTestCase):
    def test__appeal_s78_standardisation_process__run__with_no_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_prefix = "t_as78sp_r_wned"
        # h
        horizoncases_s78 = spark.createDataFrame(
            (
                generate_horizoncases_s78_row(
                    casenodeid="1",
                    subtype="1",
                    abbreviation="w",
                    casereference="1",
                    caseuniqueid="1",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="2",
                    subtype="2",
                    abbreviation="w",
                    casereference="2",
                    caseuniqueid="2",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="3",
                    subtype="3",
                    abbreviation="w",
                    casereference="3",
                    caseuniqueid="3",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="4",
                    subtype="4",
                    abbreviation="w",
                    casereference="4",
                    caseuniqueid="4",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="5",
                    subtype="5",
                    abbreviation="w",
                    casereference="5",
                    caseuniqueid="5",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="6",
                    subtype="6",
                    abbreviation="w",
                    casereference="6",
                    caseuniqueid="6",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="7",
                    subtype="7",
                    abbreviation="w",
                    casereference="7",
                    caseuniqueid="7",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="8",
                    subtype="8",
                    abbreviation="w",
                    casereference="8",
                    caseuniqueid="8",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="9",
                    subtype="9",
                    abbreviation="w",
                    casereference="9",
                    caseuniqueid="9",
                ),
                generate_horizoncases_s78_row(
                    casenodeid="10",
                    subtype="10",
                    abbreviation="w",
                    casereference="10",
                    caseuniqueid="10",
                ),
            ),
            schema=generate_horizoncases_s78_schema(),
        )
        horizoncases_s78_table = f"{test_prefix}_horizoncases_s78"
        self.write_existing_table(
            spark,
            horizoncases_s78,
            horizoncases_s78_table,
            "odw_standardised_db",
            "odw-standardised",
            horizoncases_s78_table,
            "overwrite",
        )
        # cs
        cases_specialisms = spark.createDataFrame(
            (
                generate_cases_specialisms_row(casereference="1", casespecialism="Specialism A"),
                generate_cases_specialisms_row(casereference="2", casespecialism="Specialism B"),
                generate_cases_specialisms_row(casereference="3", casespecialism="Specialism C"),
                generate_cases_specialisms_row(casereference="4", casespecialism="Specialism D"),
                generate_cases_specialisms_row(casereference="5", casespecialism="Specialism E"),
                generate_cases_specialisms_row(casereference="6", casespecialism="Specialism F"),
                generate_cases_specialisms_row(casereference="7", casespecialism="Specialism G"),
                generate_cases_specialisms_row(casereference="8", casespecialism="Specialism H"),
                generate_cases_specialisms_row(casereference="9", casespecialism="Specialism I"),
                generate_cases_specialisms_row(casereference="10", casespecialism="Specialism J"),
            ),
            schema=generate_cases_specialisms_schema(),
        )
        cases_specialisms_table = f"{test_prefix}_cases_specialisms"
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
                generate_vw_case_dates_row(casenodeid="1"),
                generate_vw_case_dates_row(casenodeid="2"),
                generate_vw_case_dates_row(casenodeid="3"),
                generate_vw_case_dates_row(casenodeid="4"),
                generate_vw_case_dates_row(casenodeid="5"),
                generate_vw_case_dates_row(casenodeid="6"),
                generate_vw_case_dates_row(casenodeid="7"),
                generate_vw_case_dates_row(casenodeid="8"),
                generate_vw_case_dates_row(casenodeid="9"),
                generate_vw_case_dates_row(casenodeid="10"),
            ),
            schema=generate_vw_case_dates_schema(),
        )
        vw_case_dates_table = f"{test_prefix}_vw_case_dates"
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
                generate_casedocumentdatesdates_row(casenodeid="1", appealrefno="1"),
                generate_casedocumentdatesdates_row(casenodeid="2", appealrefno="2"),
                generate_casedocumentdatesdates_row(casenodeid="3", appealrefno="3"),
                generate_casedocumentdatesdates_row(casenodeid="4", appealrefno="4"),
                generate_casedocumentdatesdates_row(casenodeid="5", appealrefno="5"),
                generate_casedocumentdatesdates_row(casenodeid="6", appealrefno="6"),
                generate_casedocumentdatesdates_row(casenodeid="7", appealrefno="7"),
                generate_casedocumentdatesdates_row(casenodeid="8", appealrefno="8"),
                generate_casedocumentdatesdates_row(casenodeid="9", appealrefno="9"),
                generate_casedocumentdatesdates_row(casenodeid="10", appealrefno="10"),
            ),
            schema=generate_casedocumentdatesdates_schema(),
        )
        casedocumentdatesdates_table = f"{test_prefix}_casedocumentdatesdates"
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
                generate_casesitestrings_row(
                    casenodeid="1",
                    town="townA",
                    county="townA",
                    postcode="postcodeA",
                    country="countryA",
                ),
                generate_casesitestrings_row(
                    casenodeid="2",
                    town="townB",
                    county="townB",
                    postcode="postcodeB",
                    country="countryB",
                ),
                generate_casesitestrings_row(
                    casenodeid="3",
                    town="townC",
                    county="townC",
                    postcode="postcodeC",
                    country="countryC",
                ),
                generate_casesitestrings_row(
                    casenodeid="4",
                    town="townD",
                    county="townD",
                    postcode="postcodeD",
                    country="countryD",
                ),
                generate_casesitestrings_row(
                    casenodeid="5",
                    town="townE",
                    county="townE",
                    postcode="postcodeE",
                    country="countryE",
                ),
                generate_casesitestrings_row(
                    casenodeid="6",
                    town="townF",
                    county="townF",
                    postcode="postcodeF",
                    country="countryF",
                ),
                generate_casesitestrings_row(
                    casenodeid="7",
                    town="townG",
                    county="townG",
                    postcode="postcodeG",
                    country="countryG",
                ),
                generate_casesitestrings_row(
                    casenodeid="8",
                    town="townH",
                    county="townH",
                    postcode="postcodeH",
                    country="countryH",
                ),
                generate_casesitestrings_row(
                    casenodeid="9",
                    town="townI",
                    county="townI",
                    postcode="postcodeI",
                    country="countryI",
                ),
                generate_casesitestrings_row(
                    casenodeid="10",
                    town="townJ",
                    county="townJ",
                    postcode="postcodeJ",
                    country="countrJ",
                ),
            ),
            schema=generate_casesitestrings_schema(),
        )
        casesitestrings_table = f"{test_prefix}_casesitestrings"
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
                generate_typeofprocedure_row(id="1", name="nameA", proccode="codeA"),
                generate_typeofprocedure_row(id="2", name="nameB", proccode="codeB"),
                generate_typeofprocedure_row(id="3", name="nameC", proccode="codeC"),
                generate_typeofprocedure_row(id="4", name="nameD", proccode="codeD"),
                generate_typeofprocedure_row(id="5", name="nameE", proccode="codeE"),
                generate_typeofprocedure_row(id="6", name="nameF", proccode="codeF"),
                generate_typeofprocedure_row(id="7", name="nameG", proccode="codeG"),
                generate_typeofprocedure_row(id="8", name="nameH", proccode="codeH"),
                generate_typeofprocedure_row(id="9", name="nameI", proccode="codeI"),
                generate_typeofprocedure_row(id="10", name="nameJ", proccode="codeJ"),
            ),
            schema=generate_typeofprocedure_schema(),
        )
        typeofprocedure_table = f"{test_prefix}_typeofprocedure"
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
                generate_vw_addadditionaldata_row(appealrefnumber="1", level="A"),
                generate_vw_addadditionaldata_row(appealrefnumber="2", level="B"),
                generate_vw_addadditionaldata_row(appealrefnumber="3", level="C"),
                generate_vw_addadditionaldata_row(appealrefnumber="4", level="D"),
                generate_vw_addadditionaldata_row(appealrefnumber="5", level="E"),
                generate_vw_addadditionaldata_row(appealrefnumber="6", level="F"),
                generate_vw_addadditionaldata_row(appealrefnumber="7", level="G"),
                generate_vw_addadditionaldata_row(appealrefnumber="8", level="H"),
                generate_vw_addadditionaldata_row(appealrefnumber="9", level="I"),
                generate_vw_addadditionaldata_row(appealrefnumber="10", level="J"),
            ),
            schema=generate_vw_addadditionaldata_schema(),
        )
        vw_addadditionaldata_table = f"{test_prefix}_vw_addadditionaldata"
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
                generate_vw_additionalfields_row(
                    appealrefnumber="1",
                    casereference="1",
                    importantinformation="infoA",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="2",
                    casereference="2",
                    importantinformation="infoB",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="3",
                    casereference="3",
                    importantinformation="infoC",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="4",
                    casereference="4",
                    importantinformation="infoD",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="5",
                    casereference="5",
                    importantinformation="infoE",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="6",
                    casereference="6",
                    importantinformation="infoF",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="7",
                    casereference="7",
                    importantinformation="infoG",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="8",
                    casereference="8",
                    importantinformation="infoH",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="9",
                    casereference="9",
                    importantinformation="infoI",
                ),
                generate_vw_additionalfields_row(
                    appealrefnumber="10",
                    casereference="10",
                    importantinformation="infoJ",
                ),
            ),
            schema=generate_vw_additionalfields_schema(),
        )
        vw_additionalfields_table = f"{test_prefix}_vw_additionalfields"
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
                generate_horizon_advert_attributes_row(caseNodeId=1, caseUniqueId=1, advertType="adTypeA"),
                generate_horizon_advert_attributes_row(caseNodeId=2, caseUniqueId=2, advertType="adTypeB"),
                generate_horizon_advert_attributes_row(caseNodeId=3, caseUniqueId=3, advertType="adTypeC"),
                generate_horizon_advert_attributes_row(caseNodeId=4, caseUniqueId=4, advertType="adTypeD"),
                generate_horizon_advert_attributes_row(caseNodeId=5, caseUniqueId=5, advertType="adTypeE"),
                generate_horizon_advert_attributes_row(caseNodeId=6, caseUniqueId=6, advertType="adTypeF"),
                generate_horizon_advert_attributes_row(caseNodeId=7, caseUniqueId=7, advertType="adTypeG"),
                generate_horizon_advert_attributes_row(caseNodeId=8, caseUniqueId=8, advertType="adTypeH"),
                generate_horizon_advert_attributes_row(caseNodeId=9, caseUniqueId=9, advertType="adTypeI"),
                generate_horizon_advert_attributes_row(caseNodeId=10, caseUniqueId=10, advertType="adTypeJ"),
            ),
            schema=generate_horizon_advert_attributes_schema(),
        )
        horizon_advert_attributes_table = f"{test_prefix}_horizon_advert_attributes"
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
                generate_typeoflevel_row(id=1, name="A", band="bandA"),
                generate_typeoflevel_row(id=2, name="B", band="bandB"),
                generate_typeoflevel_row(id=3, name="C", band="bandC"),
                generate_typeoflevel_row(id=4, name="D", band="bandD"),
                generate_typeoflevel_row(id=5, name="E", band="bandE"),
                generate_typeoflevel_row(id=6, name="F", band="bandF"),
                generate_typeoflevel_row(id=7, name="G", band="bandG"),
                generate_typeoflevel_row(id=8, name="H", band="bandH"),
                generate_typeoflevel_row(id=9, name="I", band="bandI"),
                generate_typeoflevel_row(id=10, name="J", band="bandJ"),
            ),
            schema=generate_typeoflevel_schema(),
        )
        typeoflevel_table = f"{test_prefix}_TypeOfLevel"
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
                generate_horizon_specialist_case_dates_row(appealrefnumber="1"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="2"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="3"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="4"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="5"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="6"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="7"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="8"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="9"),
                generate_horizon_specialist_case_dates_row(appealrefnumber="10"),
            ),
            schema=generate_horizon_specialist_case_dates_schema(),
        )
        horizon_specialist_case_dates_table = f"{test_prefix}_horizon_specialist_case_dates"
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
                generate_planningappstrings_row(casenodeid="1", planningapplicationtype="appTypeA"),
                generate_planningappstrings_row(casenodeid="2", planningapplicationtype="appTypeB"),
                generate_planningappstrings_row(casenodeid="3", planningapplicationtype="appTypeC"),
                generate_planningappstrings_row(casenodeid="4", planningapplicationtype="appTypeD"),
                generate_planningappstrings_row(casenodeid="5", planningapplicationtype="appTypeE"),
                generate_planningappstrings_row(casenodeid="6", planningapplicationtype="appTypeF"),
                generate_planningappstrings_row(casenodeid="7", planningapplicationtype="appTypeG"),
                generate_planningappstrings_row(casenodeid="8", planningapplicationtype="appTypeH"),
                generate_planningappstrings_row(casenodeid="9", planningapplicationtype="appTypeI"),
                generate_planningappstrings_row(casenodeid="10", planningapplicationtype="appTypeJ"),
            ),
            schema=generate_planningappstrings_schema(),
        )
        planningappstrings_table = f"{test_prefix}_PlanningAppStrings"
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
                generate_planningappdates_row(casenodeid="1"),
                generate_planningappdates_row(casenodeid="2"),
                generate_planningappdates_row(casenodeid="3"),
                generate_planningappdates_row(casenodeid="4"),
                generate_planningappdates_row(casenodeid="5"),
                generate_planningappdates_row(casenodeid="6"),
                generate_planningappdates_row(casenodeid="7"),
                generate_planningappdates_row(casenodeid="8"),
                generate_planningappdates_row(casenodeid="9"),
                generate_planningappdates_row(casenodeid="10"),
            ),
            schema=generate_planningappdates_schema(),
        )
        planningappdates_table = f"{test_prefix}_PlanningAppDates"
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
                generate_bis_leadcase_row(casenodeid="1", leadcasenodeid="10"),
                generate_bis_leadcase_row(casenodeid="2", leadcasenodeid="9"),
                generate_bis_leadcase_row(casenodeid="3", leadcasenodeid="8"),
                generate_bis_leadcase_row(casenodeid="4", leadcasenodeid="7"),
                generate_bis_leadcase_row(casenodeid="5", leadcasenodeid="6"),
                generate_bis_leadcase_row(casenodeid="6", leadcasenodeid="5"),
                generate_bis_leadcase_row(casenodeid="7", leadcasenodeid="4"),
                generate_bis_leadcase_row(casenodeid="8", leadcasenodeid="3"),
                generate_bis_leadcase_row(casenodeid="9", leadcasenodeid="2"),
                generate_bis_leadcase_row(casenodeid="10", leadcasenodeid="1"),
            ),
            schema=generate_bis_leadcase_schema(),
        )
        bis_leadcase_table = f"{test_prefix}_BIS_LeadCase"
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
                generate_casestrings_row(casenodeid="1", proceduretype="procTypeA"),
                generate_casestrings_row(casenodeid="2", proceduretype="procTypeB"),
                generate_casestrings_row(casenodeid="3", proceduretype="procTypeC"),
                generate_casestrings_row(casenodeid="4", proceduretype="procTypeD"),
                generate_casestrings_row(casenodeid="5", proceduretype="procTypeE"),
                generate_casestrings_row(casenodeid="6", proceduretype="procTypeF"),
                generate_casestrings_row(casenodeid="7", proceduretype="procTypeG"),
                generate_casestrings_row(casenodeid="8", proceduretype="procTypeH"),
                generate_casestrings_row(casenodeid="9", proceduretype="procTypeI"),
                generate_casestrings_row(casenodeid="10", proceduretype="procTypeJ"),
            ),
            schema=generate_casestrings_schema(),
        )
        casestrings_table = f"{test_prefix}_CaseStrings"
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
                generate_horizon_case_info_row(appealrefnumber="1", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="2", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="3", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="4", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="5", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="6", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="7", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="8", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="9", validity="Valid"),
                generate_horizon_case_info_row(appealrefnumber="10", validity="Valid"),
            ),
            schema=generate_horizon_case_info_schema(),
        )
        horizon_case_info_table = f"{test_prefix}_horizon_case_info"
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
                generate_horizon_case_dates_row(appealrefnumber="1"),
                generate_horizon_case_dates_row(appealrefnumber="2"),
                generate_horizon_case_dates_row(appealrefnumber="3"),
                generate_horizon_case_dates_row(appealrefnumber="4"),
                generate_horizon_case_dates_row(appealrefnumber="5"),
                generate_horizon_case_dates_row(appealrefnumber="6"),
                generate_horizon_case_dates_row(appealrefnumber="7"),
                generate_horizon_case_dates_row(appealrefnumber="8"),
                generate_horizon_case_dates_row(appealrefnumber="9"),
                generate_horizon_case_dates_row(appealrefnumber="10"),
            ),
            schema=generate_horizon_case_dates_schema(),
        )
        horizon_case_dates_table = f"{test_prefix}_horizon_case_dates"
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
                generate_horizon_appeals_additional_data_row(appealrefnumber="1", processingstate="Complete", appellant="Alice"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="2", processingstate="Complete", appellant="Bob"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="3", processingstate="Complete", appellant="Charlie"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="4", processingstate="Complete", appellant="Dave"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="5", processingstate="Complete", appellant="Emily"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="6", processingstate="Complete", appellant="Frank"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="7", processingstate="Complete", appellant="Garry"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="8", processingstate="Complete", appellant="Harry"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="9", processingstate="Complete", appellant="Ivy"),
                generate_horizon_appeals_additional_data_row(appealrefnumber="10", processingstate="Complete", appellant="Jim"),
            ),
            schema=generate_horizon_appeals_additional_data_schema(),
        )
        horizon_appeals_additional_data_table = f"{test_prefix}_horizon_appeals_additional_data"
        self.write_existing_table(
            spark,
            horizon_appeals_additional_data,
            horizon_appeals_additional_data_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_appeals_additional_data_table,
            "overwrite",
        )
        # hag
        horizon_appeal_grounds = spark.createDataFrame(
            (
                generate_horizon_appeal_grounds_row(caseNodeId=1, appealGroundLetter="(a)"),
                generate_horizon_appeal_grounds_row(caseNodeId=2, appealGroundLetter="(b)"),
                generate_horizon_appeal_grounds_row(caseNodeId=3, appealGroundLetter="(c)"),
                generate_horizon_appeal_grounds_row(caseNodeId=4, appealGroundLetter="(d)"),
                generate_horizon_appeal_grounds_row(caseNodeId=5, appealGroundLetter="(e)"),
                generate_horizon_appeal_grounds_row(caseNodeId=6, appealGroundLetter="(f)"),
                generate_horizon_appeal_grounds_row(caseNodeId=7, appealGroundLetter="(g)"),
                generate_horizon_appeal_grounds_row(caseNodeId=8, appealGroundLetter="(h)"),
                generate_horizon_appeal_grounds_row(caseNodeId=9, appealGroundLetter="(i)"),
                generate_horizon_appeal_grounds_row(caseNodeId=10, appealGroundLetter="(j)"),
            ),
            schema=generate_horizon_appeal_grounds_schema(),
        )
        horizon_appeal_grounds_table = f"{test_prefix}_horizon_appeal_grounds"
        self.write_existing_table(
            spark,
            horizon_appeal_grounds,
            horizon_appeal_grounds_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_appeal_grounds_table,
            "overwrite",
        )
        # hnd
        horizon_notice_dates = spark.createDataFrame(
            (
                generate_horizon_notice_dates_row(caseUniqueId=1, caseNodeId=1),
                generate_horizon_notice_dates_row(caseUniqueId=2, caseNodeId=2),
                generate_horizon_notice_dates_row(caseUniqueId=3, caseNodeId=3),
                generate_horizon_notice_dates_row(caseUniqueId=4, caseNodeId=4),
                generate_horizon_notice_dates_row(caseUniqueId=5, caseNodeId=5),
                generate_horizon_notice_dates_row(caseUniqueId=6, caseNodeId=6),
                generate_horizon_notice_dates_row(caseUniqueId=7, caseNodeId=7),
                generate_horizon_notice_dates_row(caseUniqueId=8, caseNodeId=8),
                generate_horizon_notice_dates_row(caseUniqueId=9, caseNodeId=9),
                generate_horizon_notice_dates_row(caseUniqueId=10, caseNodeId=10),
            ),
            schema=generate_horizon_notice_dates_schema(),
        )
        horizon_notice_dates_table = f"{test_prefix}_horizon_notice_dates"
        self.write_existing_table(
            spark,
            horizon_notice_dates,
            horizon_notice_dates_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_notice_dates_table,
            "overwrite",
        )
        # hmu
        horizon_application_made_under_section = spark.createDataFrame(
            (
                generate_horizon_application_made_under_section_row(caseUniqueId=1, caseNodeId=1, applicationMadeUnderSection=1),
                generate_horizon_application_made_under_section_row(caseUniqueId=2, caseNodeId=2, applicationMadeUnderSection=2),
                generate_horizon_application_made_under_section_row(caseUniqueId=3, caseNodeId=3, applicationMadeUnderSection=3),
                generate_horizon_application_made_under_section_row(caseUniqueId=4, caseNodeId=4, applicationMadeUnderSection=4),
                generate_horizon_application_made_under_section_row(caseUniqueId=5, caseNodeId=5, applicationMadeUnderSection=5),
                generate_horizon_application_made_under_section_row(caseUniqueId=6, caseNodeId=6, applicationMadeUnderSection=6),
                generate_horizon_application_made_under_section_row(caseUniqueId=7, caseNodeId=7, applicationMadeUnderSection=7),
                generate_horizon_application_made_under_section_row(caseUniqueId=8, caseNodeId=8, applicationMadeUnderSection=8),
                generate_horizon_application_made_under_section_row(caseUniqueId=9, caseNodeId=9, applicationMadeUnderSection=9),
                generate_horizon_application_made_under_section_row(caseUniqueId=10, caseNodeId=10, applicationMadeUnderSection=10),
            ),
            schema=generate_horizon_application_made_under_section_schema(),
        )
        horizon_application_made_under_section_table = f"{test_prefix}_horizon_application_made_under_section"
        self.write_existing_table(
            spark,
            horizon_application_made_under_section,
            horizon_application_made_under_section_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_application_made_under_section_table,
            "overwrite",
        )

        # Generated by feeding the test data into the original notebook and extracting the result
        expected_data = spark.createDataFrame(
            (
                generate_expected_row(
                    casenodeid="1",
                    subtype="1",
                    casereference="1",
                    caseuniqueid="1",
                    casespecialism="Specialism A",
                    importantinformation="infoA",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(a)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="A",
                    allocationBand="bandA",
                    typeOfPlanningApplication="appTypeA",
                    advertType="adTypeA",
                    leadCaseReference="10",
                    procedureType="procTypeA",
                ),
                generate_expected_row(
                    casenodeid="10",
                    subtype="10",
                    casereference="10",
                    caseuniqueid="10",
                    casespecialism="Specialism J",
                    importantinformation="infoJ",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(j)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="J",
                    allocationBand="bandJ",
                    typeOfPlanningApplication="appTypeJ",
                    advertType="adTypeJ",
                    leadCaseReference="1",
                    procedureType="procTypeJ",
                ),
                generate_expected_row(
                    casenodeid="2",
                    subtype="2",
                    casereference="2",
                    caseuniqueid="2",
                    casespecialism="Specialism B",
                    importantinformation="infoB",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(b)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="B",
                    allocationBand="bandB",
                    typeOfPlanningApplication="appTypeB",
                    advertType="adTypeB",
                    leadCaseReference="9",
                    procedureType="procTypeB",
                ),
                generate_expected_row(
                    casenodeid="3",
                    subtype="3",
                    casereference="3",
                    caseuniqueid="3",
                    casespecialism="Specialism C",
                    importantinformation="infoC",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(c)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="C",
                    allocationBand="bandC",
                    typeOfPlanningApplication="appTypeC",
                    advertType="adTypeC",
                    leadCaseReference="8",
                    procedureType="procTypeC",
                ),
                generate_expected_row(
                    casenodeid="4",
                    subtype="4",
                    casereference="4",
                    caseuniqueid="4",
                    casespecialism="Specialism D",
                    importantinformation="infoD",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(d)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="D",
                    allocationBand="bandD",
                    typeOfPlanningApplication="appTypeD",
                    advertType="adTypeD",
                    leadCaseReference="7",
                    procedureType="procTypeD",
                ),
                generate_expected_row(
                    casenodeid="5",
                    subtype="5",
                    casereference="5",
                    caseuniqueid="5",
                    casespecialism="Specialism E",
                    importantinformation="infoE",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(e)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="E",
                    allocationBand="bandE",
                    typeOfPlanningApplication="appTypeE",
                    advertType="adTypeE",
                    leadCaseReference="6",
                    procedureType="procTypeE",
                ),
                generate_expected_row(
                    casenodeid="6",
                    subtype="6",
                    casereference="6",
                    caseuniqueid="6",
                    casespecialism="Specialism F",
                    importantinformation="infoF",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(f)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="F",
                    allocationBand="bandF",
                    typeOfPlanningApplication="appTypeF",
                    advertType="adTypeF",
                    leadCaseReference="5",
                    procedureType="procTypeF",
                ),
                generate_expected_row(
                    casenodeid="7",
                    subtype="7",
                    casereference="7",
                    caseuniqueid="7",
                    casespecialism="Specialism G",
                    importantinformation="infoG",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(g)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="G",
                    allocationBand="bandG",
                    typeOfPlanningApplication="appTypeG",
                    advertType="adTypeG",
                    leadCaseReference="4",
                    procedureType="procTypeG",
                ),
                generate_expected_row(
                    casenodeid="8",
                    subtype="8",
                    casereference="8",
                    caseuniqueid="8",
                    casespecialism="Specialism H",
                    importantinformation="infoH",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(h)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="H",
                    allocationBand="bandH",
                    typeOfPlanningApplication="appTypeH",
                    advertType="adTypeH",
                    leadCaseReference="3",
                    procedureType="procTypeH",
                ),
                generate_expected_row(
                    casenodeid="9",
                    subtype="9",
                    casereference="9",
                    caseuniqueid="9",
                    casespecialism="Specialism I",
                    importantinformation="infoI",
                    enforcementAppealGroundsDetails=[{"appealGroundLetter": "(i)", "groundForAppealStartDate": datetime(2025, 1, 1)}],
                    allocationLevel="I",
                    allocationBand="bandI",
                    typeOfPlanningApplication="appTypeI",
                    advertType="adTypeI",
                    leadCaseReference="2",
                    procedureType="procTypeI",
                ),
            ),
            schema=generate_expected_schema(),
        )
        expected_output_table = f"{test_prefix}_horizon_appeal_s78"
        property_override_map = {
            "STANDARDISED_HORIZON_CASES_S78": horizoncases_s78_table,
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
            "STANDARDISED_HORIZON_APPEAL_GROUNDS": horizon_appeal_grounds_table,
            "STANDARDISED_HORIZON_NOTICE_DATES": horizon_notice_dates_table,
            "STANDARDISED_HORIZON_APPLICATION_MADE_UNDER_SECTION": horizon_application_made_under_section_table,
            "OUTPUT_TABLE": expected_output_table,
        }
        with ExitStack() as stack:
            for s78_property, override_value in property_override_map.items():
                stack.enter_context(mock.patch.object(AppealS78StandardisationProcess, s78_property, override_value))
            inst = AppealS78StandardisationProcess(spark)
            result = inst.run()
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_standardised_db.{expected_output_table}")
            assert_dataframes_equal(expected_data, actual_table_data)

    def test__appeal_s78_standardisation_process__run__with_existing_data(self):
        pass
