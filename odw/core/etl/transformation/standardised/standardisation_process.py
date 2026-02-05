from odw.core.etl.transformation.transformation_process import TransformationProcess
from odw.core.util.util import Util
from odw.core.util.azure_blob_util import AzureBlobUtil
from odw.core.etl.etl_result import ETLResult
from odw.core.etl.util.schema_util import SchemaUtil
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json
import re
from copy import deepcopy


class StandardisationProcess(TransformationProcess):
    @classmethod
    def get_name(cls):
        return "Standardisation"

    def standardise(
        self, data: DataFrame, schema: Dict[str, Any], expected_from: datetime, expected_to: datetime, process_name: str, definition: Dict[str, Any]
    ):
        """
        This replicates the functionality of the `ingest_adhoc` function from `py_1_raw_to_standardised_hr_functions`
        """
        df = data.select([col for col in data.columns if not col.startswith("Unnamed")])
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
            df = df.withColumn(col_name, col_value)

        ### change any array field to string
        standardised_table_schema = deepcopy(schema)
        for field in standardised_table_schema["fields"]:
            if field["type"] == "array":
                field["type"] = "string"
        standardised_table_schema = StructType.fromJson(standardised_table_schema)

        ### remove characters that Delta can't allow in headers and add numbers to repeated column headers
        cols_orig = df.schema.names
        cols = [re.sub("[^0-9a-zA-Z]+", "_", i).lower() for i in cols_orig]
        cols = [colm.rstrip("_") for colm in cols]
        newlist = []
        for i, v in enumerate(cols):
            totalcount = cols.count(v)
            count = cols[:i].count(v)
            newlist.append(v + str(count + 1) if totalcount > 1 else v)
        df = df.toDF(*newlist)

        ### Cast any column in df with type mismatch
        for field in df.schema:
            table_field = next((f for f in standardised_table_schema if f.name.lower() == field.name.lower()), None)
            if table_field is not None and field.dataType != table_field.dataType:
                df = df.withColumn(field.name, F.col(field.name).cast(table_field.dataType))
        return df

    def process(self, **kwargs) -> ETLResult:
        # Initialise input parameters
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError("StandardisationProcess.process requires a source_data dictionary to be provided, but was missing")
        orchestration_file: Dict[str, Any] = kwargs.get("orchestration_file", None)
        orchestration_file = deepcopy(orchestration_file)
        if not orchestration_file:
            raise ValueError("StandardisationProcess.process requires a orchestration_file json to be provided, but was missing")
        date_folder_input: str = kwargs.get("date_folder", None)
        source_frequency_folder: str = kwargs.get("source_frequency_folder")
        specific_file: str = kwargs.get("specific_file", None)  # if not provided, it will ingest all files in the date_folder
        # Initialise variables
        if date_folder_input == "":
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_input, "%Y-%m-%d")
        process_name = "py_raw_to_std"
        # Initialise source data

        definitions: List[Dict[str, Any]] = orchestration_file.pop("definitions", None)
        if not definitions:
            raise ValueError("definitions is missing")

        output_data = {}
        for file, data in source_data.items():
            definition = next(
                (
                    d
                    for d in definitions
                    if (specific_file == "" or d["Source_Filename_Start"] == specific_file)
                    and (not source_frequency_folder or d["Source_Frequency_Folder"] == source_frequency_folder)
                    and file.startswith(d["Source_Filename_Start"])
                ),
                None,
            )
            expected_from = date_folder - timedelta(days=1)
            expected_from = datetime.combine(expected_from, datetime.min.time())
            expected_to = expected_from + timedelta(days=definition["Expected_Within_Weekdays"])
            if "Standardised_Table_Definition" in definition:
                standardised_table_loc = Util.get_path_to_file(f"odw-config/{definition['Standardised_Table_Definition']}")
                standardised_table_schema = json.loads(self.spark.read.text(standardised_table_loc, wholetext=True).first().value)
            else:
                standardised_table_schema = SchemaUtil(db_name="odw_standardised_db").get_schema_for_entity(definition["Source_Frequency_Folder"])
            df = self.standardise(data, standardised_table_schema, expected_from, expected_to, process_name, definition)
            output_data[file] = df
        return output_data

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        data_to_read: List[Dict[str, Any]] = kwargs.get("data_to_read", None)
        if not data_to_read:
            raise ValueError("StandardisationProcess expected a data_to_read parameter to be passed, but this was missing")
        data_map = super().load_data(data_to_read)
        # Fetch the contents of the orchestration file
        storage_account = Util.get_storage_account()
        orchestration_file_bytes = AzureBlobUtil(storage_endpoint=storage_account).read("odw-config", "orchestration/orchestration.json")
        orchestration_file = json.load(orchestration_file_bytes)
        data_map["orchestration_file"] = orchestration_file
        return data_map
