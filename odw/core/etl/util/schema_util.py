from odw.core.util.logging_util import LoggingUtil
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
