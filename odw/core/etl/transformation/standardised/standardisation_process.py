from odw.core.etl.transformation.transformation_process import TransformationProcess
from odw.core.util.util import Util
from odw.core.util.azure_blob_util import AzureBlobUtil
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.table_util import TableUtil
from odw.core.etl.etl_result import ETLResult, ETLResultFactory
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta, date
from typing import Dict, List, Any
import json


class StandardisationProcess(TransformationProcess):
    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()
        # Initialise input parameters
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError(f"StandardisationProcess.process requires a source_data dictoinary to be provided, but was missing")
        orchestration_file: str = source_data.get("orchestration_file", None)
        date_folder_input: str = kwargs.get("orchestration_file", None)
        source_folder: str = kwargs.get("source_folder", None)
        source_frequency_folder: str = kwargs.get("source_frequency_folder")
        specific_file: str = kwargs.get("specific_file", None) # if not provided, it will ingest all files in the date_folder
        is_multiLine: bool = kwargs.get("is_multiLine", True)
        delete_existing_table: bool = kwargs.get("delete_existing_table", False)
        data_attribute: str = kwargs.get("data_attribute", None)
        # Initialise source data
        orchestration_file: Dict[str, Any] = kwargs.get("orchestration_file", None)
        
        # Initialise variables
        spark = SparkSession.builder.getOrCreate()
        insert_count = 0
        update_count = 0
        delete_count = 0
        process_name = "py_raw_to_std"
        if date_folder_input == '':
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_input, "%Y-%m-%d")
        storage_account = Util.get_storage_account()
        raw_container = "abfss://odw-raw@" + storage_account
        source_folder_path = source_folder if not source_frequency_folder else f"{source_folder}/{source_frequency_folder}"
        source_path = f"{raw_container}{source_folder_path}/{date_folder.strftime('%Y-%m-%d')}"

        definitions: List[Dict[str, Any]] = orchestration_file.get("definitions", None)
        if not definition:
            print("LOG ERROR HERE")

        files_to_process = AzureBlobUtil(storage_endpoint=storage_account).list_blobs("odw-raw", source_folder_path)
        for file_name in files_to_process:
            # ignore json raw files if source is service bus
            if source_folder == "ServiceBus" and file_name.endswith(".json"):
                continue

            # ignore files other than specified file 
            if specific_file != "" and not file_name.startswith(specific_file + "."):
                continue

            definition = next(
                (
                    d
                    for d in definitions
                    if (specific_file == "" or d["Source_Filename_Start"] == specific_file) and
                       (not source_frequency_folder or d["Source_Frequency_Folder"] == source_frequency_folder) and
                       file_name.startswith(d["Source_Filename_Start"]
                    )
                ),
                None
            )
            if definition:
                expected_from = date_folder - timedelta(days=1)
                expected_from = datetime.combine(expected_from, datetime.min.time())
                expected_to = expected_from + timedelta(days=definition["Expected_Within_Weekdays"])
                if delete_existing_table:
                    LoggingUtil().log_info(f"Deleting existing table if exists odw_standardised_db.{definition['Standardised_Table_Name']}")
                    TableUtil().delete_table(db_name="odw_standardised_db", table_name=definition["Standardised_Table_Name"])
                LoggingUtil().log_info(f"Ingesting {file_name}")
                # Todo need to refactor the ingest_adhoc function
                (ingestion_failure, row_count) = ingest_adhoc(storage_account, definition, source_path, file_name, expected_from, expected_to, process_name, is_multiLine, data_attribute)
                insert_count = row_count
                end_exec_time = datetime.now()
                duration_seconds = (end_exec_time - start_exec_time).total_seconds()
                activity_type = f"{self.get_name()} ETLProcess"
                ingestion_outcome = "Success" if not ingestion_failure else "Failed"
                status_message = (
                    f"Successfully loaded data into {definition['Standardised_Table_Name']} table"
                    if not ingestion_failure
                    else f"Failed to load data from {definition['Standardised_Table_Name']} table"
                )
                LoggingUtil().log_info(f"StandardisationProcess Ingested {row_count} rows")
                # Todo need to handle the logging/error handling, but this should be done in the parent notebook

    def load_data(self, data_to_read: List[Dict[str, Any]]) -> Dict[str, DataFrame]:
        data_map = super().load_data(data_to_read)
        # Fetch the contents of the orchestration file
        storage_account = Util.get_storage_account()
        orchestration_file_bytes = AzureBlobUtil(storage_endpoint=storage_account).read("odw-config", "orchestration/orchestration.json")
        orchestration_file = json.load(orchestration_file_bytes)
        data_map["orchestration_file"] = orchestration_file
        return data_map
