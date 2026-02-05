from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.etl.util.schema_util import SchemaUtil
from notebookutils import mssparkutils
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException
from datetime import datetime, timedelta
from typing import Dict, List, Any
import re
import json
from copy import deepcopy


class HorizonStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising the raw data from the Horizon

    # Example usage

    ```
    params = {
        "entity_stage_name": "Horizon Standardisation",
        "source_folder": "Horizon",  # Default is Horizon, but this could be any folder in the `odw-raw` container
        "entity_name": "",  # Default is "". Aligns with the `Source_Filename_Start` property in the orchestration file
    }
    HorizonStandardisationProcess(spark).run(**params)
    ```
    """

    @classmethod
    def get_name(cls):
        return "Horizon Standardisation"

    def get_last_modified_folder(self, path: str):
        # List all items in the directory
        items = mssparkutils.fs.ls(path)
        # Filter for directories and get their names and modification times
        folders = [(item.name, item.modifyTime) for item in items if item.isDir]
        # Sort folders by modification time in descending order
        sorted_folders = sorted(folders, key=lambda x: x[1], reverse=True)
        # Get the name of the latest modified folder
        if sorted_folders:
            latest_folder = sorted_folders[0][0]
            return latest_folder

    def get_file_names_in_directory(self, path: str) -> List[str]:
        files = mssparkutils.fs.ls(path)
        return [file.name for file in files]

    def load_data(self, **kwargs):
        source_folder = self.load_parameter("source_folder", kwargs, "Horizon")
        entity_name = self.load_parameter("entity_name", kwargs, "")
        source_path = Util.get_path_to_file(f"odw-raw/{source_folder}")
        last_modified_folder = self.get_last_modified_folder(source_path)
        if last_modified_folder:
            source_path += f"/{last_modified_folder}"

        file_map = dict()

        # Load new raw data files to add to the existing data
        horizon_files = self.get_file_names_in_directory(source_path)
        for file in horizon_files:
            data = SynapseFileDataIO().read(
                spark=self.spark,
                storage_endpoint=Util.get_storage_account(),
                container_name="odw-raw",
                blob_path=f"{source_folder}/{last_modified_folder}/{file}",
                file_format="csv",
                read_options={
                    "quote": '"',
                    "escape": "\\",
                    "encoding": "utf8",
                    "header": True,
                    "multiLine": True,
                    "columnNameOfCorruptRecord": "corrupted_records",
                    "mode": "PERMISSIVE",
                },
            )
            if "corrupted_records" in data.columns:
                raise RuntimeError(f"Failed to load file '{file}': The file had corrupt records after being read")
            file_map[file] = data

        # Load orchestration file
        orchestration_data = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=Util.get_storage_account(),
            container_name="odw-config",
            blob_path="orchestration/orchestration.json",
            file_format="json",
            read_options={"multiline": "true"},
        )
        file_map["orchestration_data"] = orchestration_data

        # Load existing data (if any)
        definitions: List[Dict[str, Any]] = json.loads(orchestration_data.toJSON().first())["definitions"]
        for file in horizon_files:
            definition = next(
                (
                    d
                    for d in definitions
                    if (entity_name == "" or d.get("Source_Filename_Start", None) == entity_name)
                    and file.startswith(d.get("Source_Filename_Start", None))
                    and d.get("Load_Enable_status", False) == "True"
                ),
                None,
            )
            if definition:
                table_name = definition.get("Standardised_Table_Name", None)
                if not table_name:
                    raise ValueError(f"Orchestration entry for '{file}' does not have a 'Standardised_Table_Name' property")
                new_entry_name = f"odw_standardised_db.{table_name}"
                try:
                    data = SynapseTableDataIO().read(
                        spark=self.spark, database_name="odw_standardised_db", table_name=table_name, file_format="delta"
                    )
                    file_map[new_entry_name] = data
                except AnalysisException:
                    file_map[new_entry_name] = None
                # Load standardised table schema
                if "Standardised_Table_Definition" in definition:
                    standardised_table_loc = Util.get_path_to_file(f"odw-config/{definition['Standardised_Table_Definition']}")
                    standardised_table_schema = json.loads(self.spark.read.text(standardised_table_loc, wholetext=True).first().value)
                else:
                    standardised_table_schema = SchemaUtil(db_name="odw_standardised_db").get_schema_for_entity(definition["Source_Frequency_Folder"])
                file_map[f"{table_name}_standardised_table_schema"] = standardised_table_schema
        return file_map

    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        entity_name = self.load_parameter("entity_name", kwargs, "")
        date_folder_input = self.load_parameter("date_folder", kwargs, "")
        if date_folder_input == "":
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_input, "%Y-%m-%d")
        # Only process the csv files
        files_to_process = {k: v for k, v in source_data.items() if k != "orchestration_data" and k.endswith(".csv")}
        orchestration_data: DataFrame = self.load_parameter("orchestration_data", source_data)
        definitions: List[Dict[str, Any]] = json.loads(orchestration_data.toJSON().first())["definitions"]

        processed_tables = []
        new_row_count = 0

        data_to_write = dict()
        for file in files_to_process.keys():
            definition = next(
                (
                    d
                    for d in definitions
                    if (entity_name == "" or d.get("Source_Filename_Start", None) == entity_name)
                    and file.startswith(d.get("Source_Filename_Start", None))
                    and d.get("Load_Enable_status", False) == "True"
                ),
                None,
            )

            if definition:
                # Recreation of ingest adhoc
                table_name = definition.get("Standardised_Table_Name", None)
                processed_tables.append(table_name)
                schema = source_data.get(f"{table_name}_standardised_table_schema")
                expected_from = date_folder - timedelta(days=1)
                expected_from = datetime.combine(expected_from, datetime.min.time())
                expected_to = expected_from + timedelta(days=definition["Expected_Within_Weekdays"])
                process_name = "horizon_standardisation_process"
                data = files_to_process[file]
                col_value_map = {
                    "ingested_datetime": F.current_timestamp(),
                    "ingested_by_process_name": F.lit(process_name),
                    "expected_from": F.lit(expected_from),
                    "expected_to": F.lit(expected_to),
                    "input_file": F.input_file_name(),
                    "modified_datetime": F.current_timestamp(),
                    "modified_by_process_name": F.lit(process_name),
                    "entity_name": F.lit(definition["Source_Filename_Start"]),
                    "file_ID": F.sha2(F.concat(F.lit(F.input_file_name()), F.current_timestamp().cast("string")), 256),
                }
                for col_name, col_value in col_value_map.items():
                    data = data.withColumn(col_name, col_value)
                ### change any array field to string
                standardised_table_schema = deepcopy(schema)
                for field in standardised_table_schema["fields"]:
                    if field["type"] == "array":
                        field["type"] = "string"
                standardised_table_schema = StructType.fromJson(standardised_table_schema)

                cols_orig = data.schema.names
                cols = [re.sub("[^0-9a-zA-Z]+", "_", i).lower() for i in cols_orig]
                cols = [colm.rstrip("_") for colm in cols]
                newlist = []
                for i, v in enumerate(cols):
                    totalcount = cols.count(v)
                    count = cols[:i].count(v)
                    newlist.append(v + str(count + 1) if totalcount > 1 else v)
                data = data.toDF(*newlist)

                ### Cast any column in df with type mismatch
                for field in data.schema:
                    table_field = next((f for f in standardised_table_schema if f.name.lower() == field.name.lower()), None)
                    if table_field is not None and field.dataType != table_field.dataType:
                        data = data.withColumn(field.name, F.col(field.name).cast(table_field.dataType))

                table_exists = source_data.get(f"odw_standardised_db.{table_name}", None) is not None
                write_mode = "append" if table_exists else "overwrite"
                write_opts = [("mergeSchema", "true")] if table_exists else []

                new_row_count += data.count()
                data_to_write[f"odw_standardised_db.{table_name}"] = {
                    "data": data,
                    "storage_kind": "ADLSG2-Table",
                    "database_name": "odw_standardised_db",
                    "table_name": table_name,
                    "storage_endpoint": Util.get_storage_account(),
                    "container_name": "odw-standardised",
                    "blob_path": f"Horizon/{table_name}",
                    "file_format": "delta",
                    "write_mode": write_mode,
                    "write_options": write_opts,
                }
            else:
                if entity_name:
                    raise RuntimeError(f"No definition found for {file}")
                # Do we want to just crash instead? Not sure why the NB does this
                LoggingUtil().log_info(f"Condition Not Satisfied for Load {file} File")
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
