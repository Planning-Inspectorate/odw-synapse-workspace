from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.anonymisation.engine import AnonymisationEngine
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Dict
from datetime import datetime


class HistoricalAnonymisationProcess(ETLProcess):
    SOURCE_CONTAINER = "odw-standardised"
    SINK_CONTAINER = "odw-anonymised"
    _ENTITY_CONFIG = {
        "nsip-document": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "nsip-exam-timetable": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "nsip-representation": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-event": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "application-update": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "nsip-project-update": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "service-user": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "nsip-project": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-has": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-s78": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "nsip-subscription": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-event-estimate": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "s51-advice": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-document": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "folder": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "appeal-representation": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "ServiceBus",
        },  # Service bus
        "AIEDocumentData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "AIEDocumentData",
        },  # From py_raw_to_std
        "entraid": {
            "raw_blob_path": "entraid",
            "raw_blob_format": "json",
            "standardised_blob_path": "",
            "category": "entraid",
            "primary_keys": ["id"],
            "cols_to_revert_to_raw": ["userPrincipalName"],
        },  # From py_raw_to_std
        "SpecialistCaseDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseInfo": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "InspectorCases": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "PlanningAppDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "Horizon_AppealGrounds": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "S62AViewCaseOfficers": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "Horizon_NoticeDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "horizon_ApplicationMadeUnderSection": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "DaRT_Inspectors": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "vw_case_dates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseStrings": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "AppealsAdditionalData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonAppealsDocumentMetadata": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "cases_specialisms": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "NSIPAdvice": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "S62AViewCases": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "S62AViewCaseExtendedData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonAppealsEvent": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseDocumentDatesDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "vw_AdditionalFields": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "BIS_LeadCase": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonAppealsFolder": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseInvolvement": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "TypeOfLevel": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonAdvertAttributes": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "DaRT_LPA": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonFolder": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonCases_s78": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "ExaminationTimetable": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "vw_AddAdditionalData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "NSIPReleventRepresentation": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "BIS_CaseSiteCategoryAdditionalStr": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "HorizonCases_Has": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "TypeOfProcedure": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "DocumentMetaData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "Horizon_TypeOfReasonForCase": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "CaseSiteStrings": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "S62AViewCaseBasicData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "PlanningAppStrings": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "S62AViewCaseDates": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
        "NSIPData": {
            "raw_blob_path": "",
            "raw_blob_format": "",
            "standardised_blob_path": "",
            "category": "Horizon",
        },  # Horizon
    }

    @classmethod
    def get_name(cls):
        return "Historical Data Anonymisation Process"

    def load_data(self, **kwargs):
        entity_name = kwargs.get("entity_name", "")
        if not entity_name:
            raise ValueError(
                f"HistoricalAnonymisationProcess requires an 'entity_name' argument to be provided"
            )
        entity_config = self._ENTITY_CONFIG.get(entity_name, None)
        if not entity_config:
            raise ValueError(
                f"Could not find an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG for entity '{entity_name}'"
            )
        raw_blob_path = entity_config.get("raw_blob_path", "")
        raw_blob_format = entity_config.get("raw_blob_format", "")
        raw_blob_read_options = entity_config.get("raw_blob_read_options", dict())
        standardised_blob_path = entity_config.get("standardised_blob_path", "")
        storage_endpoint = Util.get_storage_account()
        standardised_data = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=storage_endpoint,
            container_name="odw-standardised",
            blob_path=standardised_blob_path,
            file_format="parquet",
        )
        raw_blob_path_cleaned = SynapseFileDataIO()._format_to_adls_path(
            "odw-raw", raw_blob_path, storage_endpoint=storage_endpoint
        )
        prefix = raw_blob_path_cleaned.replace(raw_blob_path, "")
        raw_blob_paths = [
            x.replace(prefix, "")
            for x in self.get_all_files_in_directory(raw_blob_path_cleaned)
            if x.endswith(".csv")
        ]
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
                f"HistoricalAnonymisationProcess requires an 'entity_name' argument to be provided"
            )
        entity_config = self._ENTITY_CONFIG.get(entity_name, None)
        if not entity_config:
            raise ValueError(
                f"Could not find an entry in HistoricalAnonymisationProcess._ENTITY_CONFIG for entity '{entity_name}'"
            )
        entity_category = entity_config.get("category")
        standardised_blob_path = entity_config.get("standardised_blob_path", "")
        primary_keys = entity_config.get("primary_keys")
        if not primary_keys:
            raise ValueError(f"No primary keys specified")
        cols_to_revert_to_raw = entity_config.get("cols_to_revert_to_raw", [])
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        standardised_data: DataFrame = self.load_parameter(
            "standardised_data", source_data
        )
        raw_data: DataFrame = self.load_parameter("raw_data", source_data)
        joined = standardised_data.alias("standardised_data").join(
            raw_data.alias("raw_data"), on=primary_keys, how="left"
        )
        cols_to_keep = [
            F.col(f"standardised_data.{x}")
            for x in standardised_data.columns
            if x not in cols_to_revert_to_raw
        ] + [F.col(f"raw_data.{x}") for x in cols_to_revert_to_raw]
        standardised_data_cleaned = joined.select(*cols_to_keep)
        # cols_to_anonymise = AnonymisationEngine().get_cols_to_anonymise(entity_name, source_folder=entity_category)
        anonymised_data = AnonymisationEngine().apply_from_purview(
            standardised_data_cleaned,
            entity_name=entity_name,
            source_folder=entity_category,
        )
        end_exec_time = datetime.now()
        data_to_write = {
            entity_name: {
                "data": anonymised_data,
                "storage_kind": "ADLSG2-File",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": f"anonymised/{standardised_blob_path}",
                "file_format": "parquet",
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
