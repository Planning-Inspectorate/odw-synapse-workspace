from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.anonymisation.engine import AnonymisationEngine
from odw.core.util.logging_util import LoggingUtil
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from typing import Dict
from datetime import datetime
from typing import List
from functools import reduce
import json
import os


class HistoricalAnonymisationProcess(ETLProcess):
    SOURCE_CONTAINER = "odw-standardised"
    SINK_CONTAINER = "odw-anonymised"
    _ENTITY_CONFIG = {
        # "nsip-document": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/nsip-document",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_nsip_document",
        #     "category": "ServiceBus",
        #     "primary_keys": ["documentId"]
        # },  # Service bus
        # "nsip-exam-timetable": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/nsip-exam-timetable",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_nsip_exam_timetable",
        #     "category": "ServiceBus",
        #     "primary_keys": ["caseReference"]
        # },  # Service bus
        # "nsip-representation": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/nsip-representation",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_nsip_representation",
        #     "category": "ServiceBus",
        #     "primary_keys": ["representationId"]
        # },  # Service bus
        # "appeal-event": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/appeal-event",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_appeal_event",
        #     "category": "ServiceBus",
        #     "primary_keys": ["eventId"]
        # },  # Service bus
        # "application-update": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/application-update",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_application_update",
        #     "category": "ServiceBus",
        #     "primary_keys": ["id"]
        # },  # Service bus
        # "nsip-project-update": { # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/nsip-project-update",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_nsip_project_update",
        #     "category": "ServiceBus",
        #     "primary_keys": ["id"]
        # },  # Service bus
        # "service-user": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/service-user",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_service_user",
        #     "category": "ServiceBus",
        #     "primary_keys": ["id"]
        # },  # Service bus
        # "nsip-project": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/nsip-project",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_nsip_project",
        #     "category": "ServiceBus",
        #     "primary_keys": ["caseId"]
        # },  # Service bus
        "appeal-has": {
            "raw_blob_path": "ServiceBus/appeal-has",
            "raw_blob_format": "json",
            "standardised_blob_path": "sb_appeal_has",
            "category": "ServiceBus",
            "primary_keys": ["caseReference"],
            "cols_to_revert_to_raw": ["siteAddressTown", "newConditionDetails"],
        },  # Service bus
        # "appeal-s78": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/appeal-s78",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_appeal_s78",
        #     "category": "ServiceBus",
        #     "primary_keys": ["caseReference"]
        # },  # Service bus
        "nsip-subscription": {
            "raw_blob_path": "ServiceBus/nsip-subscription",
            "raw_blob_format": "json",
            "standardised_blob_path": "sb_nsip_subscription",
            "category": "ServiceBus",
            "primary_keys": ["subscriptionId", "message_enqueued_time_utc"],
            "cols_to_revert_to_raw": ["emailAddress"],
        },  # Service bus
        # "appeal-event-estimate": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/appeal-event-estimate",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_appeal_event_estimate",
        #     "category": "ServiceBus",
        #     "primary_keys": ["id"]
        # },  # Service bus
        # "s51-advice": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/s51-advice",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_s51_advice",
        #     "category": "ServiceBus",
        #     "primary_keys": ["adviceId"]
        # },  # Service bus
        # "appeal-document": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/appeal-document",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_appeal_document",
        #     "category": "ServiceBus",
        #     "primary_keys": ["documentId"]
        # },  # Service bus
        # "folder": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/folder",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_folder",
        #     "category": "ServiceBus",
        #     "primary_keys": ["id"]
        # },  # Service bus
        # "appeal-representation": {  # OUT OF SCOPE
        #     "raw_blob_path": "ServiceBus/appeal-representation",
        #     "raw_blob_format": "json",
        #     "standardised_blob_path": "sb_appeal_representation",
        #     "category": "ServiceBus",
        #     "primary_keys": ["representationId"]
        # },  # Service bus
        # "AIEDocumentData": {  # OUT OF SCOPE
        #     "raw_blob_path": "AIEDocumentData",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "AIEDocumentData/aie_document_data",
        #     "category": "AIEDocumentData",
        #     "primary_keys": ["documentId", "filename", "documentURI", "dateCreated"],
        #     "cols_to_revert_to_raw": ["path"],
        # },  # From py_raw_to_std
        "entraid": {
            "raw_blob_path": "entraid",
            "raw_blob_format": "json",
            "standardised_blob_path": "entraid",
            "category": "entraid",
            "primary_keys": ["id"],
            "cols_to_revert_to_raw": ["userPrincipalName"],
        },  # From py_raw_to_std
        # "SpecialistCaseDates": { # OUT OF SCOPE
        #    "raw_blob_path": "Horizon",
        #    "raw_blob_format": "",
        #    "standardised_blob_path": "Horizon/horizon_specialist_case_dates",
        #    "category": "Horizon",
        # },  # Horizon
        # "CaseInfo": { # OUT OF SCOPE
        #    "raw_blob_path": "Horizon",
        #    "raw_blob_format": "",
        #    "standardised_blob_path": "Horizon/horizon_case_info",
        #    "category": "Horizon",
        # },  # Horizon
        "InspectorCases": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/horizon_inspector_cases",
            "category": "Horizon",
            "horizon_file_name": "InspectorCases.csv",
            "primary_keys": [
                "appealrefnumber",
                "casereference",
                "contactid",
                "parentkeyid",
            ],
            "cols_to_revert_to_raw": [
                "InspectorName",  # Seems to be missing from standardised
                "FirstName",
                "LastName",
                "Email",
            ],
        },  # Horizon
        # "PlanningAppDates": { # OUT OF SCOPE
        #    "raw_blob_path": "Horizon",
        #    "raw_blob_format": "",
        #    "standardised_blob_path": "Horizon/PlanningAppDates",  # This one breaks from the convention for some reason
        #    "category": "Horizon",
        # },  # Horizon
        # "Horizon_AppealGrounds": { # OUT OF SCOPE
        #    "raw_blob_path": "Horizon",
        #    "raw_blob_format": "",
        #    "standardised_blob_path": "Horizon/horizon_appeal_grounds",
        #    "category": "Horizon",
        # },  # Horizon
        # "S62AViewCaseOfficers": { # OUT OF SCOPE
        #    "raw_blob_path": "Horizon",
        #    "raw_blob_format": "",
        #    "standardised_blob_path": "Horizon/horizon_s62a_view_case_officers",
        #    "category": "Horizon",
        # },  # Horizon
        # "CaseDates": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_case_dates",
        #     "category": "Horizon",
        #     "primary_keys": [],
        #     "horizon_file_name": "CaseDates.csv",
        #     "cols_to_revert_to_raw": ["ValidityStatusDate"],  # todo check
        # },  # Horizon
        # "Horizon_NoticeDates": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_notice_dates",
        #     "category": "Horizon",
        # },  # Horizon
        # "horizon_ApplicationMadeUnderSection": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/application_made_under_section",
        #     "category": "Horizon",
        # },  # Horizon
        "DaRT_Inspectors": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/horizon_pins_inspector",  # double check this one
            "category": "Horizon",
            "cols_to_revert_to_raw": ["firstName", "lastName", "salutation", "email"],
            "primary_keys": [
                "horizonId",
                "postName",
                "organisationName",
                "title",
                "qualifications",
            ],
            "horizon_file_name": "DaRT_Inspectors.csv",
        },  # Horizon
        # "vw_case_dates": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/vw_case_dates",
        #     "category": "Horizon",
        # },  # Horizon
        # "CaseStrings": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/CaseStrings",  # This one breaks from the convention for some reason
        #     "category": "Horizon",
        # },  # Horizon
        # "AppealsAdditionalData": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/appeals_additional_data",
        #     "category": "Horizon",
        # },  # Horizon
        # "HorizonAppealsDocumentMetadata": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/appeals_document_metadata",
        #     "category": "Horizon",
        # },  # Horizon
        # "cases_specialisms": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/cases_specialisms",
        #     "category": "Horizon",
        # },  # Horizon
        # "NSIPAdvice": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_nsip_advice",
        #     "category": "Horizon",
        # },  # Horizon
        # "S62AViewCases": {  # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_s62a_view_cases",
        #     "category": "Horizon",
        # },  # Horizon
        # "S62AViewCaseExtendedData": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_s62a_view_case_extended_data",
        #     "category": "Horizon",
        # },  # Horizon
        # "HorizonAppealsEvent": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/horizon_appeals_event",
        #     "category": "Horizon",
        # },  # Horizon
        # "CaseDocumentDatesDates": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "",
        #     "standardised_blob_path": "Horizon/CaseDocumentDatesDates",
        #     "category": "Horizon",
        # },  # Horizon
        # "vw_AdditionalFields": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/vw_AdditionalFields",
        #     "category": "Horizon",
        # },  # Horizon
        # "BIS_LeadCase": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/BIS_LeadCase",
        #     "category": "Horizon",
        # },  # Horizon
        # "HorizonAppealsFolder": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_appeals_folder",
        #     "category": "Horizon",
        # },  # Horizon
        "CaseInvolvement": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/horizon_case_involvement",
            "category": "Horizon",
            "primary_keys": ["case_number", "case_created_date", "POBox", "Fax"],
            "horizon_file_name": "CaseInvolvement.csv",
            "cols_to_revert_to_raw": [
                "FirstName",
                "LastName",
                "Address1",
                "Address2",
                "City",
                "Postcode",
                "Email",
            ],
        },  # Horizon
        # "TypeOfLevel": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/TypeOfLevel",
        #     "category": "Horizon",
        # },  # Horizon
        # "HorizonAdvertAttributes": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_advert_attributes",
        #     "category": "Horizon",
        # },  # Horizon
        "DaRT_LPA": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "pins_lpa",
            "category": "Horizon",
            "horizon_file_name": "DaRT_LPA.csv",
            "primary_keys": [
                "lpaName",
                "organisationType",
                "pinsLpaCode",
                "poBox",
                "county",
            ],
            "cols_to_revert_to_raw": [
                "address1",
                "address2",
                "city",
                "postcode",
                "emailAddress",
            ],
        },  # Horizon
        # "HorizonFolder": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_folder",
        #     "category": "Horizon",
        # },  # Horizon
        "HorizonCases_s78": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/HorizonCases_s78",
            "category": "Horizon",
            "primary_keys": [
                "CaseNodeId",
                "Abbreviation",
                "caseReference",
                "caseUniqueId",
            ],
            "horizon_file_name": "HorizonCases_s78.csv",
            "cols_to_revert_to_raw": ["caseOfficerName", "coEmailAddress"],
        },  # Horizon
        # "ExaminationTimetable": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_examination_timetable",
        #     "category": "Horizon",
        # },  # Horizon
        # "vw_AddAdditionalData": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/vw_AddAdditionalData",
        #     "category": "Horizon",
        # },  # Horizon
        "NSIPReleventRepresentation": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/horizon_nsip_relevant_representation",
            "category": "Horizon",
            "primary_keys": [
                "CaseReference",
                "CaseNodeId",
                "CaseUniqueId",
                "ContactID",
            ],
            "horizon_file_name": "NSIPReleventRepresentation.csv",
            "cols_to_revert_to_raw": ["FullName", "EmailAddress"],
        },  # Horizon
        # "BIS_CaseSiteCategoryAdditionalStr": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/BIS_CaseSiteCategoryAdditionalStr",
        #     "category": "Horizon",
        # },  # Horizon
        "HorizonCases_Has": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/HorizonCases_Has",
            "category": "Horizon",
            "primary_keys": [
                "CaseNodeId",
                "caseReference",
                "caseUniqueId",
                "caseOfficerId",
            ],
            "horizon_file_name": "HorizonCases_Has.csv",
            "cols_to_revert_to_raw": ["caseOfficerName", "coEmailAddress"],
        },  # Horizon
        # "TypeOfProcedure": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/TypeOfProcedure",
        #     "category": "Horizon",
        # },  # Horizon
        "DocumentMetaData": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/document_meta_data",
            "primary_keys": [
                "VersionID",
                "CaseNodeId",
                "caseReference",
                "documentReference",
            ],
            "horizon_file_name": "DocumentMetaData.csv",
            "category": "Horizon",
            "cols_to_revert_to_raw": ["representative"],
        },  # Horizon
        # "Horizon_TypeOfReasonForCase": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/Horizon_TypeOfReasonForCase",
        #     "category": "Horizon",
        # },  # Horizon
        "CaseSiteStrings": {
            "raw_blob_path": "Horizon",
            "raw_blob_format": "csv",
            "standardised_blob_path": "Horizon/CaseSiteStrings",
            "horizon_file_name": "CaseSiteStrings.csv",
            "primary_keys": ["CaseNodeId", "AreaOfSite", "County"],
            "category": "Horizon",
            "cols_to_revert_to_raw": ["LandUse", "Town"],
        },  # Horizon
        # "S62AViewCaseBasicData": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_s62a_view_case_basic_data",
        #     "category": "Horizon",
        # },  # Horizon
        # "PlanningAppStrings": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/PlanningAppStrings",
        #     "category": "Horizon",
        # },  # Horizon
        # "S62AViewCaseDates": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_s62a_view_case_dates",
        #     "category": "Horizon",
        # },  # Horizon
        # "NSIPData": { # OUT OF SCOPE
        #     "raw_blob_path": "Horizon",
        #     "raw_blob_format": "csv",
        #     "standardised_blob_path": "Horizon/horizon_nsip_data",
        #     "category": "Horizon",
        # },  # Horizon
    }

    @classmethod
    def get_name(cls):
        return "Historical Data Anonymisation Process"

    def load_data(self, **kwargs):
        entity_name = kwargs.get("entity_name", "")
        if not entity_name:
            raise ValueError(
                "HistoricalAnonymisationProcess requires an 'entity_name' argument to be provided"
            )
        entity_config = self._ENTITY_CONFIG.get(entity_name, None)
        if not entity_config:
            raise ValueError(
                f"Could not find an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG for entity '{entity_name}'"
            )
        # Load all of the standardised data for the entity
        standardised_blob_path = entity_config.get("standardised_blob_path", "")
        storage_endpoint = Util.get_storage_account()
        standardised_data = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=storage_endpoint,
            container_name="odw-standardised",
            blob_path=standardised_blob_path,
            file_format="parquet",
        )
        raw_blob_path = entity_config.get("raw_blob_path", "")
        raw_blob_format = entity_config.get("raw_blob_format", "")
        read_options_map = {"csv": {"header": "true"}, "json": {"multiline": "true"}}
        raw_blob_read_options = read_options_map.get(raw_blob_format, None)
        if not raw_blob_read_options:
            raise ValueError(
                f"No read options defined for file format '{raw_blob_format}'"
            )
        raw_blob_path_cleaned = SynapseFileDataIO()._format_to_adls_path(
            "odw-raw", raw_blob_path, storage_endpoint=storage_endpoint
        )
        # Get the adlsg2 prefix (i.e. "abds://<container>@<storage_endpoint>"")
        prefix = raw_blob_path_cleaned.replace(raw_blob_path, "")
        # Generate a list of raw files to be extracted
        raw_blob_paths = [
            x.replace(prefix, "")
            for x in self.get_all_files_in_directory(raw_blob_path_cleaned)
            if x.endswith(f".{raw_blob_format}")
            and entity_name
            in os.path.splitext(os.path.basename(x.replace(prefix, "")))[0]
        ]
        # Load all of the found raw data files
        raw_data = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=storage_endpoint,
            container_name="odw-raw",
            blob_path=raw_blob_paths,
            file_format=raw_blob_format,
            read_options=raw_blob_read_options,
        )
        return {"standardised_data": standardised_data, "raw_data": raw_data}

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        entity_name = kwargs.get("entity_name", "")
        if not entity_name:
            raise ValueError(
                "HistoricalAnonymisationProcess requires an 'entity_name' argument to be provided"
            )
        # Load the entity config
        entity_config = self._ENTITY_CONFIG.get(entity_name, None)
        if not entity_config:
            raise ValueError(
                f"Could not find an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG for entity '{entity_name}'"
            )
        entity_category = entity_config.get("category")
        standardised_blob_path = entity_config.get("standardised_blob_path", "")
        cols_to_revert_to_raw = entity_config.get("cols_to_revert_to_raw", [])
        horizon_file_name = entity_config.get("horizon_file_name", None)
        if entity_category == "Horizon" and not horizon_file_name:
            # The anonymisation engine requires a filename to be specified - this is the underlying name of the csv file that gets ingested into the ODW
            raise ValueError(
                "For Horizon entities, the config must have a 'horizon_file_name' entry"
            )
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        standardised_data: DataFrame = self.load_parameter(
            "standardised_data", source_data
        )
        raw_data: DataFrame = self.load_parameter("raw_data", source_data)
        standardised_data_count = standardised_data.count()
        primary_keys = entity_config.get("primary_keys")
        if not primary_keys:
            raise ValueError(
                f"Require a primary_key field to be populated for the entry '{entity_name}'"
            )
        LoggingUtil().log_info(
            f"The raw data contains the following columns: {json.dumps(raw_data.columns, indent=4)}"
        )
        LoggingUtil().log_info(
            f"The standardised data contains the following columns: {json.dumps(standardised_data.columns, indent=4)}"
        )
        # Most data sources ingest updates to the data as rows with duplicate PKs. It's basically impossible to determine which
        # value to keep, and the end value doesn't really matter in the grand scheme of things since it will be replaced by an anonymised value anyway,
        # so we just drop the duplicates and let pyspark choose the row to keep
        raw_data_cleaned = raw_data.dropDuplicates(primary_keys)
        # Join the standardised data back with the raw data
        joined = standardised_data.alias("standardised_data").join(
            raw_data_cleaned.alias("raw_data"), on=primary_keys, how="left"
        )
        # Detect join explosion - there should be exactly one value per row
        joined_data_count = joined.count()
        if joined_data_count != standardised_data_count:
            raise ValueError(
                f"The row count of the joined data is different to the standardised data - please check the primary key is unique. Standardised has {standardised_data_count} rows, the joined data has {joined_data_count} rows"
            )
        # Replace the specified columns with the raw version of the column
        cols_to_revert_to_raw_lower = [x.lower() for x in cols_to_revert_to_raw]
        cols_to_keep = [
            F.col(f"standardised_data.{x}")
            if x.lower() not in cols_to_revert_to_raw_lower
            else F.col(f"raw_data.{x}")
            for x in standardised_data.columns
        ]
        standardised_data_cleaned = joined.select(*cols_to_keep)
        # Reanonymise the data
        anonymised_data = AnonymisationEngine().apply_from_purview(
            standardised_data_cleaned,
            entity_name=entity_name,
            source_folder=entity_category,
            file_name=horizon_file_name,
        )
        end_exec_time = datetime.now()
        data_to_write = {
            entity_name: {
                "data": anonymised_data,
                "storage_kind": "ADLSG2-File",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": f"anonymised/{standardised_blob_path}",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=entity_name,
                insert_count=0,
                update_count=standardised_data_cleaned.count(),
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )

    def validate_all_anonymised_data(self, entity_name: str, path_prefix: str = ""):
        """
        Verify if the given entity has been correctly anonymised. If there is a validation error, then the invalid rows are displayed and returned.

        :param entity_name str: The entity to verify. Must align with an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG
        :param path_prefix str: Override prefix to add to the standardised path. This is useful for annymised data written in the "anonymised/" folder
        :return Union[DataFrame|None]: The unanonymised data, or none if the data is anonymised correctly
        """
        storage_endpoint = Util.get_storage_account()
        entity_config = self._ENTITY_CONFIG.get(entity_name, None)
        if not entity_config:
            raise ValueError(
                f"Could not find an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG for entity '{entity_name}'"
            )
        standardised_blob_path = entity_config.get("standardised_blob_path", "")
        if path_prefix:
            standardised_blob_path = f"{path_prefix}/{standardised_blob_path}"
        standardised_data = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=storage_endpoint,
            container_name="odw-standardised",
            blob_path=standardised_blob_path,
            file_format="parquet",
        )
        cols_to_revert_to_raw = entity_config.get("cols_to_revert_to_raw")
        # We can only practically verify the columns that are of string type, since non-strings get converted to random values
        standardised_schema = {
            field.name.lower(): field.dataType for field in standardised_data.schema
        }
        conditions = [
            F.col(c).isNotNull() & F.col(c).rlike("^[A-Za-z\d_-]\*+")
            for c in cols_to_revert_to_raw
            if isinstance(standardised_schema[c.lower()], T.StringType)
        ]
        invalid_data = standardised_data.filter(reduce(lambda x, y: x | y, conditions))
        if invalid_data.count() > 0:
            Util.display_dataframe(invalid_data)
            return invalid_data
        return None
