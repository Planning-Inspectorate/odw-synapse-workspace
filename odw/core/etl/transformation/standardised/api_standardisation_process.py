from odw.core.etl.transformation.standardised.standardisation_process import (
    StandardisationProcess,
)
from notebookutils import mssparkutils
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from typing import Dict, List
import re
import json


class APIStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising the raw data from API-based data sources

    Note this process is very similar to HorizonStandardisationProcess, so would be good to further refactor the code to reduce code duplication.
    For now this is just migrated from the py_raw_to_std notebook to allow AIE data to be ingested by the new pipeline
    """

    @classmethod
    def get_name(cls) -> str:
        return "API Standardisation Process"

    def get_file_names_in_directory(self, path: str) -> List[str]:
        LoggingUtil().log_info(
            f"Attempting to list the file names of the location: '{path}'"
        )
        files = mssparkutils.fs.ls(path)
        return [file.name for file in files]

    def load_data(self, **kwargs):
        # Note this is left mostly unchanged from the original notebook - further refactoring is possible
        date_folder_in = kwargs.get("date_folder", "")
        source_folder = kwargs.get("source_folder", "")  # AIEDocumentData
        source_frequency_folder = kwargs.get("source_frequency_folder", "")
        specific_file = kwargs.get(
            "specific_file", ""
        )  # if not provided, it will ingest all files in the date_folder
        is_multiline = kwargs.get("is_multiline", True)

        storage_account = Util.get_storage_account()

        if date_folder_in == "":
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_in, "%Y-%m-%d")

        date_folder_str = date_folder.strftime("%Y-%m-%d")
        source_folder_path = (
            source_folder
            if not source_frequency_folder
            else f"{source_folder}/{source_frequency_folder}"
        )

        # Read orchestration data
        df = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=Util.get_storage_account(),
            container_name="odw-config",
            blob_path="orchestration/orchestration.json",
            file_format="json",
            read_options={"multiline": "true"},
        )
        definitions = json.loads(df.toJSON().first())["definitions"]

        source_path = Util.get_path_to_file(
            f"odw-raw/{source_folder_path}/{date_folder_str}"
        )

        # Detect files to be extracted
        LoggingUtil().log_info(f"Reading from {source_path}")
        files = self.get_file_names_in_directory(source_path)
        LoggingUtil().log_info(
            f"Found the following files: {json.dumps(files, indent=4)}"
        )

        json_read_options = {"multiline": True} if is_multiline else dict()
        csv_read_options = {
            "quote": '"',
            "escape": "\\",
            "encoding": "utf8",
            "header": True,
            "multiLine": True,
            "columnNameOfCorruptRecord": "corrupted_records",
            "mode": "PERMISSIVE",
        }

        source_data = {"definitions": definitions}

        for file_name in files:
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
                    if (
                        specific_file == ""
                        or d["Source_Filename_Start"] == specific_file
                    )
                    and (
                        not source_frequency_folder
                        or d["Source_Frequency_Folder"] == source_frequency_folder
                    )
                    and file_name.startswith(d["Source_Filename_Start"])
                ),
                None,
            )
            LoggingUtil().log_info(
                f"Found definition for file {file_name}: {definition}"
            )

            if definition:
                expected_from = date_folder - timedelta(days=1)
                expected_from = datetime.combine(expected_from, datetime.min.time())
                # Try to load the existing standardised table
                table_name = definition["Standardised_Table_Name"]
                new_entry_name = f"odw_standardised_db.{table_name}"
                if new_entry_name not in source_data:
                    try:
                        data = SynapseTableDataIO().read(
                            spark=self.spark,
                            database_name="odw_standardised_db",
                            table_name=table_name,
                            file_format="delta",
                        )
                        source_data[new_entry_name] = data
                    except AnalysisException:
                        source_data[new_entry_name] = None
                standardised_table_definition_path = definition.get(
                    "Standardised_Table_Definition"
                )
                if not standardised_table_definition_path:
                    raise ValueError(
                        "Definition for file does not contain a 'Standardised_Table_Definition'"
                    )
                standardised_table_def_text = (
                    SynapseFileDataIO()
                    .read(
                        spark=self.spark,
                        storage_endpoint=Util.get_storage_account(),
                        container_name="odw-config",
                        blob_path=standardised_table_definition_path,
                        file_format="text",
                        read_options={"wholetext": True},
                    )
                    .first()
                    .value
                )

                LoggingUtil().log_info(f"Ingesting {file_name}")
                file_to_read = f"{source_path}/{file_name}"
                if "csv" in file_name.lower():
                    df = self.spark.read.options(**csv_read_options).csv(file_to_read)
                elif ".json" in file_name.lower():
                    df = self.spark.read.options(**json_read_options).json(
                        f"{source_path}/{file_name}"
                    )
                else:
                    raise RuntimeError(f"The file type for {file_name} is unsupported")
                source_data[file_name] = df
                source_data[f"{file_name}_standardised_table_definition"] = json.loads(
                    standardised_table_def_text
                )
        return source_data

    def process(self, **kwargs):
        # Note this is left mostly unchanged from the original notebook - further refactoring is possible
        start_exec_time = datetime.now()
        date_folder_in = kwargs.get("date_folder", "")
        if date_folder_in == "":
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_in, "%Y-%m-%d")
        source_folder = kwargs.get("source_folder", "")
        process_name = self.get_name()
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError(
                "APIStandardisationProcess.process requires a source_data dictionary to be provided, but was missing"
            )
        standardised_table_definitions = {
            k.rstrip("_standardised_table_definition"): v
            for k, v in source_data.items()
            if k.endswith("_standardised_table_definition")
        }
        LoggingUtil().log_info(
            f"The following standardised table definitions were extracted: {json.dumps(list(standardised_table_definitions.keys()), indent=4)}"
        )
        raw_data_files = {
            k: v
            for k, v in source_data.items()
            if not k.endswith("_standardised_table_definition")
        }
        LoggingUtil().log_info(
            f"The following raw data files were extracted: {json.dumps(list(raw_data_files.keys()), indent=4)}"
        )
        definitions = source_data.get("definitions")
        LoggingUtil().log_info(
            f"The following definitions were loaded: {json.dumps(definitions, indent=4)}"
        )
        source_frequency_folder = kwargs.get("source_frequency_folder", "")
        specific_file = kwargs.get(
            "specific_file", ""
        )  # if not provided, it will ingest all files in the date_folder
        processed_tables = []
        new_row_count = 0
        data_to_write = dict()
        for file_name, df in raw_data_files.items():
            definition = next(
                (
                    d
                    for d in definitions
                    if (
                        specific_file == ""
                        or d["Source_Filename_Start"] == specific_file
                    )
                    and (
                        not source_frequency_folder
                        or d["Source_Frequency_Folder"] == source_frequency_folder
                    )
                    and file_name.startswith(d["Source_Filename_Start"])
                ),
                None,
            )
            LoggingUtil().log_info(
                f"Found definition for file {file_name}: {definition}"
            )
            if definition:
                source_filename_start = definition["Source_Filename_Start"]
                standardised_table_name = definition["Standardised_Table_Name"]
                processed_tables.append(standardised_table_name)
                expected_from = date_folder - timedelta(days=1)
                expected_from = datetime.combine(expected_from, datetime.min.time())
                expected_to = expected_from + timedelta(
                    days=definition["Expected_Within_Weekdays"]
                )
                # Remove redundant cols
                df_cleaned = df.select(
                    [
                        col
                        for col in df.columns
                        if not (col.startswith("Unnamed") or col.startswith("@odata"))
                    ]
                )
                # Add standardised cols
                standardised_cols = {
                    "ingested_datetime": F.current_timestamp(),
                    "ingested_by_process_name": F.lit(process_name),
                    "expected_from": F.lit(expected_from),
                    "expected_to": F.lit(expected_to),
                    "input_file": F.input_file_name(),
                    "modified_datetime": F.current_timestamp(),
                    "modified_by_process_name": F.lit(process_name),
                    "entity_name": F.lit(source_filename_start),
                    "file_ID": F.sha2(
                        F.concat(
                            F.lit(F.input_file_name()),
                            F.current_timestamp().cast("string"),
                        ),
                        256,
                    ),
                }
                df_cleaned = df_cleaned.withColumns(standardised_cols)
                # Change any array field to string
                standardised_schema_json = standardised_table_definitions[file_name]
                for field in standardised_schema_json["fields"]:
                    if field["type"] == "array":
                        field["type"] = "string"
                standardised_schema = StructType.fromJson(standardised_schema_json)
                # Remove characters that Delta can't allow in headers and add numbers to repeated column headers
                cols_orig = df_cleaned.schema.names
                cols_cleaned = [
                    re.sub("[^0-9a-zA-Z]+", "_", i).lower().rstrip("_")
                    for i in cols_orig
                ]

                def _clean_reoccurring_columns(i, elem, cols: list):
                    total_column_occurrences = cols.count(elem)
                    currently_evaluated_occurrences = cols[:i].count(elem)
                    return (
                        elem + str(currently_evaluated_occurrences + 1)
                        if total_column_occurrences > 1
                        else elem
                    )

                cols_cleaned = [
                    _clean_reoccurring_columns(i, v, cols_cleaned)
                    for i, v in enumerate(cols_cleaned)
                ]
                df_cleaned = df_cleaned.toDF(*cols_cleaned)
                # Cast any column in df_cleaned with type mismatch
                mismatched_fields = {}
                for field in df_cleaned.schema:
                    table_field = next(
                        (
                            f
                            for f in standardised_schema
                            if f.name.lower() == field.name.lower()
                        ),
                        None,
                    )
                    if (
                        table_field is not None
                        and field.dataType != table_field.dataType
                    ):
                        mismatched_fields[field.name] = F.col(field.name).cast(
                            table_field.dataType
                        )
                df_cleaned = df_cleaned.withColumns(mismatched_fields)
                new_row_count += df_cleaned.count()
                table_exists = (
                    source_data.get(
                        f"odw_standardised_db.{standardised_table_name}", None
                    )
                    is not None
                )
                # Apply anonymisation only in DEV/TEST environments
                df_cleaned = self.try_anonymise_data(
                    df_cleaned, file_name, source_folder, source_filename_start
                )
                # Would be good to combine the dataframes into a single dataframe, and do a proper delta merge.
                # This is left as-is for now to save time/for compatibility, but it is quite inefficient
                data_to_write[f"odw_standardised_db.{standardised_table_name}"] = {
                    "data": df_cleaned,
                    "storage_kind": "ADLSG2-Table",
                    "database_name": "odw_standardised_db",
                    "table_name": standardised_table_name,
                    "storage_endpoint": Util.get_storage_account(),
                    "container_name": "odw-standardised",
                    "blob_path": standardised_table_name,
                    "file_format": "delta",
                    "write_mode": "append" if table_exists else "overwrite",
                    "write_options": {"mergeSchema": "true"}
                    if table_exists
                    else dict(),
                }
        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=", ".join(processed_tables),
                insert_count=new_row_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
