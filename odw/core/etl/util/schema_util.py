from odw.core.util.logging_util import LoggingUtil
import pyspark.sql.types as T
import requests
from typing import Dict, Any


class SchemaUtil:
    """
    Contains functions for schema operations for the ODW data.
    This is a recreation of the `py_get_schema_from_url` notebook
    """

    def __init__(self, db_name: str, incremental_key: str = None):
        self.db_name = db_name
        self.incremental_key = incremental_key
        self.data_model_version = "2.11.0"

    @LoggingUtil.logging_to_appins
    def _get_schema_from_url(self, url: str) -> Dict[str, Any]:
        try:
            response: requests.Response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                LoggingUtil().log_info("Failed to fetch data from URL. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            LoggingUtil().log_error("Error fetching data:", e)

    @LoggingUtil.logging_to_appins
    def _get_type(self, column: dict) -> str:
        if "type" not in column:
            return "string"
        elif isinstance(column["type"], list):
            column["type"] = column["type"][0]
            return self._get_type(column)
        elif column["type"] == "integer":
            return "long"
        else:
            return column["type"]

    @LoggingUtil.logging_to_appins
    def _convert_to_datalake_schema(self, schema: dict) -> dict:
        data_model: dict = {"fields": []}

        for key in schema["properties"].keys():
            value: dict = schema["properties"][key]
            data_model["fields"].append(
                {
                    "metadata": {},
                    "name": key,
                    # type doesn't exist for all fields, hence adding this check
                    "type": self._get_type(value),
                    "nullable": "type" not in value or "null" in value.get("type", []),
                }
            )

        if self.db_name == "odw_standardised_db":
            data_model["fields"].extend(
                [
                    {"metadata": {}, "name": col_name, "type": col_type, "nullable": nullable}
                    for col_name, col_type, nullable in (
                        ("ingested_datetime", "timestamp", False),
                        ("expected_from", "timestamp", False),
                        ("expected_to", "timestamp", False),
                        ("message_id", "string", False),
                        ("message_type", "string", False),
                        ("message_enqueued_time_utc", "string", False),
                    )
                ]
            )

        elif self.db_name == "odw_harmonised_db":
            if self.incremental_key:
                data_model["fields"].insert(0, {"metadata": {}, "name": self.incremental_key, "type": "string", "nullable": False})
            data_model["fields"].extend(
                [
                    {"metadata": {}, "name": col_name, "type": col_type, "nullable": nullable}
                    for col_name, col_type, nullable in (
                        ("Migrated", "string", False),
                        ("ODTSourceSystem", "string", True),
                        ("SourceSystemID", "string", True),
                        ("IngestionDate", "string", True),
                        ("ValidTo", "string", True),
                        ("RowID", "string", True),
                        ("IsActive", "string", True),
                    )
                ]
            )
        return data_model
    
    def _get_spark_type(self, field_schema: dict, definitions: dict) -> T.DataType:
        """
        Copied as-is from py_create_spark_schema
        """
        type_mapping = {
            "string": T.StringType(),
            "number": T.DoubleType(),
            "integer": T.LongType(),
            "boolean": T.BooleanType(),
            "null": T.NullType(),
            "date-time": T.TimestampType(),
            "timestamp": T.TimestampType() 
        }
        try:
            json_type = field_schema.get('type')
            print(f"Processing type: {json_type} | schema: {field_schema}")

            # Try resolving $ref if type is missing
            if json_type is None and '$ref' in field_schema:
                ref = field_schema['$ref']
                ref_path = ref.split('/')
                if ref_path[0] == '#' and ref_path[1] == '$defs':
                    resolved = definitions[ref_path[2]]
                    return self._get_spark_type(resolved, definitions)

            if json_type is None:
                raise ValueError(f"Missing 'type' in schema: {field_schema}")

            if isinstance(json_type, list):
                json_type = json_type[0]

            if json_type == 'array':
                element_schema = field_schema['items']
                return T.ArrayType(self._get_spark_type(element_schema, definitions))
            elif json_type == 'object':
                return self._transform_service_bus_schema(field_schema, definitions)
            elif json_type in type_mapping:
                return type_mapping[json_type]
            else:
                raise ValueError(f"Unsupported type: {json_type}")
        except Exception as e:
            LoggingUtil().log_exception(e)
            raise
    
    def _transform_service_bus_schema(self, schema: dict, definitions: dict) -> T.StructType:
        """
        Copied as-is from py_create_spark_schema
        """
        fields = [
            T.StructField(
                field_name,
                self._get_spark_type(field_schema, definitions),
                "null" in field_schema.get("type", []) if isinstance(field_schema.get("type"), list) else False
            )
            for field_name, field_schema in schema["properties"].items()
            if field_schema != {}
        ]
        return T.StructType(fields)

    def _resolve_refs(self, schema: dict, definitions: dict) -> dict:
        """
        Copied as-is from py_create_spark_schema
        """
        if isinstance(schema, dict):
            if "$ref" in schema:
                ref = schema["$ref"]
                ref_path = ref.split("/")
                if ref_path[0] == "#" and ref_path[1] == "$defs":
                    return self._resolve_refs(definitions[ref_path[2]], definitions)
            return {k: self._resolve_refs(v, definitions) for k, v in schema.items()}
        elif isinstance(schema, list):
            return [self._resolve_refs(item, definitions) for item in schema]
        else:
            return schema

    def _add_standardised_columns_to_schema(schema: T.StructType) -> T.StructType:
        """
        Add columns for the Standardised layer
        """
        standardised_fields = T.StructType([
            T.StructField("ingested_datetime", T.TimestampType(), False),
            T.StructField("expected_from", T.TimestampType(), False),
            T.StructField("expected_to", T.TimestampType(), False),
            T.StructField("message_id", T.StringType(), False),
            T.StructField("message_type", T.StringType(), False),
            T.StructField("message_enqueued_time_utc", T.StringType(), False),
            T.StructField("input_file", T.StringType(), False)
        ])
        master_fields: T.StructType = standardised_fields
        all_fields: list = schema.fields + master_fields.fields
        return T.StructType(all_fields)

    def _add_harmonised_columns_to_schema(schema: T.StructType, incremental_key_field: T.StructType) -> T.StructType:
        """
        Add columns for the Harmonised layer
        """
        harmonised_fields = T.StructType([
            T.StructField("migrated", T.StringType(), False),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("IngestionDate", T.StringType(), True),
            T.StructField("ValidTo", T.StringType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True)
        ])
        master_fields: T.StructType = harmonised_fields
        all_fields: list = schema.fields + master_fields.fields
        if incremental_key_field:
            all_fields.insert(0, incremental_key_field.fields[0])

        return T.StructType(all_fields)

    @LoggingUtil.logging_to_appins
    def get_schema_for_entity(self, entity_name: str):
        """
        Download the schema for the given entity from the data-model repository, and format it for ODW ETL processes

        :return Json: A json object with the below structure
        ```
        {
            "fields": [
                {
                    "metadata": {},
                    "name": "colName",  # The name of the column
                    "type": "string",  # The datatype of the column
                    "nullable": True  # The nullability of the column
                },
                ...
            ]
        }
        ```
        """
        # Need to see if we can instead return the schema as a model object from data-model
        url: str = f"https://raw.githubusercontent.com/Planning-Inspectorate/data-model/main/schemas/{entity_name}.schema.json"
        LoggingUtil().log_info(f"Reading schema from {url}")
        schema: dict = self._get_schema_from_url(url)

        if not schema:
            raise Exception(f"No schema defined at '{url}' for entity '{entity_name}'")
        LoggingUtil().log_info("Schema read")
        return self._convert_to_datalake_schema(schema)

    def get_service_bus_schema(self, entity_name: str):
        """
        Generate the schema for a service bus entity. This replicates the py_create_spark_schema notebook

        :return Json: A json object with the below structure
        ```
        {
            "fields": [
                {
                    "metadata": {},
                    "name": "colName",  # The name of the column
                    "type": "string",  # The datatype of the column
                    "nullable": True  # The nullability of the column
                },
                ...
            ]
        }
        ```
        """
        if not self.db_name:
            raise ValueError("Missing db_name")
        url: str = f"https://raw.githubusercontent.com/Planning-Inspectorate/data-model/refs/tags/{self.data_model_version}/schemas/{entity_name}.schema.json"
        LoggingUtil().log_info(f"Reading schema from {url}")
        schema = self._get_schema_from_url(url)
        if not schema:
            raise RuntimeError(f"Could not get schema for entity '{entity_name}' at url '{url}'")
        definitions = schema.get("$defs", {})
        if self.incremental_key:
            LoggingUtil().log_info("Adding incremental key")
            incremental_key_field = T.StructType([
                T.StructField(self.incremental_key, T.LongType(), False)
            ])
        else:
            incremental_key_field = None
        cleaned_schema = self._resolve_refs(schema, definitions)
        cleaned_schema = self._transform_service_bus_schema(cleaned_schema, definitions)
        if self.db_name == "odw_standardised_db":
            return self._add_standardised_columns_to_schema(schema)
        if self.db_name == "odw_harmonised_db":
            return self._add_harmonised_columns_to_schema(schema, incremental_key_field)
        return cleaned_schema
