### Module odw.core.util.util

from notebookutils import mssparkutils
from notebookutils import visualization
from pyspark.sql import DataFrame
import re
import os


class Util:
    """
    Class that defines utility functions
    """

    @classmethod
    def get_storage_account(cls) -> str:
        """
        Return the storage account of the Synapse workspace in the format `{storage_name}.dfs.core.windows.net/`
        """
        connection_string = mssparkutils.credentials.getFullConnectionString("ls_storage")
        return re.search("url=https://(.+?);", connection_string).group(1)

    @classmethod
    def get_path_to_file(cls, path: str):
        path_split = path.split("/", maxsplit=1)
        if not len(path_split) == 2:
            raise ValueError(f"Path should have the format 'container_name/path' but was '{path}'")
        container_name = path_split[0]
        blob_path = path_split[1]
        storage_account = cls.get_storage_account()
        return f"abfss://{container_name}@{storage_account}{blob_path}"
    
    @classmethod
    def display_dataframe(cls, dataframe: DataFrame):
        """
        Show the contents of the given dataframe.
        """
        visualization.display(dataframe)


### Module odw.core.etl.etl_result

from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Type, ClassVar, Optional
from datetime import datetime


class ETLResult(BaseModel, ABC):
    class ETLResultMetadata(BaseModel):
        start_execution_time: datetime
        end_execution_time: datetime
        exception: Optional[str] = None
        exception_trace: Optional[str] = None
        table_name: Optional[str] = None
        insert_count: int = Field(default_factory=0)
        update_count: int = Field(default_factory=0)
        delete_count: int = Field(default_factory=0)
        activity_type: str
        duration_seconds: float

    """
    Holds the details of the executionn of an ETLProcess, for use in logging
    """

    @property
    @abstractmethod
    def outcome(self) -> str:
        """The outcome of the ETL process"""

    @property
    @abstractmethod
    def status_code(self) -> int:
        """The status code of the ETL process"""

    metadata: ETLResultMetadata


class ETLSuccessResult(ETLResult):
    """
    For successful ETLProcesses
    """

    outcome: ClassVar[str] = "Succeeded"
    status_code: ClassVar[int] = 200


class ETLFailResult(ETLResult):
    """
    For unsuccessful ETLProcesses
    """

    outcome: ClassVar[str] = "Failed"
    status_code: ClassVar[int] = 500


class ETLResultFactory:
    """
    Automatically generate an ETLResult class from an outcome string
    """

    ETLFailResult.outcome

    @classmethod
    def get(cls, result_outcome: str) -> Type[ETLResult]:
        result_map = {result_class.outcome: result_class for result_class in (ETLSuccessResult, ETLFailResult)}
        if result_outcome not in result_map:
            raise ValueError(f"No ETLResult could be found for outcome '{result_outcome}'")
        return result_map[result_outcome]


### Module odw.core.util.logging_util

import logging
import functools
import uuid
from notebookutils import mssparkutils
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from tenacity import retry, wait_exponential, stop_after_delay
from tenacity.before_sleep import before_sleep_nothing
import threading
import requests
from datetime import datetime, timezone
from typing import Dict, Any


class LoggingUtil:
    """
    Singleton logging utility class that provides functionality to send logs to app insights.

    Example usage
    ```
    #COMMENTOUT from odw.core.util.logging_util import LoggingUtil
    LoggingUtil().log_info("Some logging message)
    @LoggingUtil.logging_to_appins
    def my_function_that_will_have_automatic_logging_applied():
        pass
    ```

    This is based on
    https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/monitor/azure-monitor-opentelemetry-exporter/samples/logs/sample_log.py
    """

    _INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls._INSTANCE:
            cls._INSTANCE = super(LoggingUtil, cls).__new__(cls, *args, **kwargs)
            cls._INSTANCE._initialise()
        return cls._INSTANCE

    def _initialise(self):
        """
        Create a `LoggingUtil` instance. Only 1 instance is ever created, which is reused.

        __init__ cannot be used because it is always called by __new__, even if cls._INSTANCE is not None
        """
        self.LOGGER_PROVIDER = LoggerProvider()
        self._LOGGING_INITIALISED = False
        self.pipelinejobid = (
            mssparkutils.runtime.context["pipelinejobid"] if mssparkutils.runtime.context.get("isForPipeline", False) else uuid.uuid4()
        )
        self.logger = logging.getLogger()
        for h in list(self.logger.handlers):
            if isinstance(h, LoggingHandler):
                self.logger.removeHandler(h)
        self.setup_logging()
        self.flush_logging()

    def log_info(self, msg: str):
        """
        Log an information message
        """
        self.logger.info(f"{self.pipelinejobid} : {msg}")

    def log_error(self, msg: str):
        """
        Log an error message string
        """
        self.logger.error(f"{self.pipelinejobid} : {msg}")

    def log_exception(self, ex: Exception):
        """
        Log an exception
        """
        self.logger.exception(f"{self.pipelinejobid} : {ex}")
    
    def save_pipeline_logs(self, payload: Dict[str, Any], event_name: str):
        """
        Submit telemetry data from a pipeline.
        This is a copy/paste from the `py_applicationinsights` notebook - further refactoring would be beneficial
        """
        endpoint = "https://uksouth-1.in.applicationinsights.azure.com/v2/track"
        payload = {
            "name": "Microsoft.ApplicationInsights.Event",
            "time": datetime.now(timezone.utc).isoformat() + "Z",
            "iKey": self.instrumentation_key,
            "data": {
                "baseType": "EventData",
                "baseData": {
                    "name": event_name,
                    "properties": payload
                }
            }
        }
        try:
            response = requests.post(endpoint, json=payload)
            print("Telemetry sent:", response.status_code)
        except Exception as e:
            print("Failed to send telemetry:", e)

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_delay(20), reraise=True, before_sleep=before_sleep_nothing)
    def setup_logging(self, force=False):
        """
        Initialise logging to Azure App Insights
        """
        if self._LOGGING_INITIALISED and not force:
            self.log_info("Logging already initialised.")
            return
        key = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        if not key:
            raise RuntimeError("The credential returned by mssparkutils.credentials.getSecretWithLS was blank or None")
        conn_string = key.split(";")[0]
        self.instrumentation_key = key.split("InstrumentationKey=")[-1].split(";")[0]

        set_logger_provider(self.LOGGER_PROVIDER)
        exporter = AzureMonitorLogExporter.from_connection_string(conn_string)
        self.LOGGER_PROVIDER.add_log_record_processor(BatchLogRecordProcessor(exporter, schedule_delay_millis=5000))

        if not any(isinstance(h, LoggingHandler) for h in self.logger.handlers):
            self.logger.addHandler(LoggingHandler())

        if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            self.logger.addHandler(logging.StreamHandler())

        self.logger.setLevel(logging.INFO)
        self._LOGGING_INITIALISED = True
        self.log_info("Logging initialised.")

    def flush_logging(self, timeout_seconds: int = 60):
        """
        Attempt to flush logs to Azure App Insights
        """
        print("Calling flush")
        event = threading.Event()

        def flush_logging_inner():
            print("Flushing logs")
            try:
                self.LOGGER_PROVIDER.force_flush()
            except Exception as e:
                print(f"Flush failed: {e}")
            event.set()

        t = threading.Thread(target=flush_logging_inner)
        t.daemon = True
        t.start()

        finished = event.wait(timeout=timeout_seconds)
        if not finished:
            print(f"force_flush() hung for >{timeout_seconds}s - continuing anyway")
        else:
            print("force_flush() completed")

    @classmethod
    def logging_to_appins(cls, func):
        """
        Decorator that adds extra logging to function calls

        Example usage
        ```
        @LoggingUtil.logging_to_appins
        def my_function_that_will_be_logged(param_a, param_b):
            ...
        ```

        ```
        @classmethod
        @LoggingUtil.logging_to_appins
        def my_class_method_that_will_be_logged(cls, param_a, param_b):
            ...
        ```
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logging_util = LoggingUtil()
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            logging_util.log_info(f"Function {func.__name__} called with args: {', '.join(args_repr + kwargs_repr)}")
            try:
                return func(*args, **kwargs)
            except mssparkutils.handlers.notebookHandler.NotebookExit as e:
                logging_util.log_info(f"Notebook exited: {e}")
                mssparkutils.notebook.exit(e)
            except Exception as e:
                logging_util.log_exception(e)
                raise

        return wrapper


### Module odw.core.exceptions

class DuplicateDataIONameException(Exception):
    pass


class DataIONameNotFoundException(Exception):
    pass


class DuplicateETLProcessNameException(Exception):
    pass


class ETLProcessNameNotFoundException(Exception):
    pass


class DuplicateDataFileParserNameException(Exception):
    pass


class DataFileParserNameNotFoundException(Exception):
    pass


### Module odw.core.util.udf


import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf
def absolute(col: int):
    if not isinstance(col, int):
        return None
    if col < 0:
        return 0 - col
    return col


### Module odw.core.util.azure_blob_util

from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient
from io import BytesIO


class AzureBlobUtil():
    def __init__(self, storage_name: str = None, storage_endpoint: str = None):
        if not (storage_name or storage_endpoint):
            raise ValueError(f"Expected one of 'storage_name' or 'storage_endpoint' to be provided to AzureBlobUtil()")
        if storage_name and storage_endpoint:
            raise ValueError(f"Expected only one of 'storage_name' or 'storage_endpoint' to be provided to AzureBlobUtil(), not both")
        self.storage_name = storage_name
        self.credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            AzureCliCredential()
        )
        if storage_endpoint:
            self.storage_endpoint = storage_endpoint
        else:
            self.storage_endpoint = f"https://{self.storage_name}.blob.core.windows.net"

    def read(self, container_name: str, blob_path: str) -> BytesIO:
        blob_service_client = BlobServiceClient(self.storage_endpoint, credential=self.credential)
        container_client = blob_service_client.get_container_client(container_name)
        byte_stream = BytesIO()
        blob_data = container_client.download_blob(blob_path)
        blob_data.readinto(byte_stream)
        return byte_stream
    
    def write(self, data_bytes: BytesIO, container_name: str, blob_path: str):
        blob_service_client = BlobServiceClient(self.storage_endpoint, credential=self.credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(data_bytes, blob_type="BlockBlob")
    
    def list_blobs(self, container_name: str, blob_path: str = ''):
        blob_service_client = BlobServiceClient(self.storage_endpoint, credential=self.credential)
        container_client = blob_service_client.get_container_client(container_name)
        blob_names = [blob.name for blob in container_client.list_blobs(name_starts_with=blob_path)]
        # list_blobs also returns a blank blob object which represents the directory itself, this blob is not wanted
        blob_names_filtered = [name for name in blob_names if not name.endswith(blob_path)]
        return blob_names_filtered


### Module odw.core.io.data_io

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class DataIO(ABC):
    """
    Abstract class to manage IO for data to/from storage
    """
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        pass

    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame
        
        :return DataFrame: The data
        """
    
    @abstractmethod
    def write(self, data: DataFrame, **kwargs):
        """
        Write the given data to the given storage location
        
        :param DataFrame data: The data to write
        """


### Module odw.core.etl.util.schema_util

#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
import pyspark.sql.types as T
import requests
from typing import Dict, Any


class SchemaUtil():
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

    def _add_standardised_columns_to_schema(self, schema: T.StructType) -> T.StructType:
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
        all_fields: list = schema.fields + standardised_fields.fields
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
        all_fields: list = schema.fields + harmonised_fields.fields
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
            return self._add_standardised_columns_to_schema(cleaned_schema)
        if self.db_name == "odw_harmonised_db":
            return self._add_harmonised_columns_to_schema(cleaned_schema, incremental_key_field)
        return cleaned_schema


### Module odw.core.util.table_util

import logging
from pyspark.sql import SparkSession
from notebookutils import mssparkutils
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil


logger = logging.getLogger(__name__)


class TableUtil:
    """
    Utility class for interacting with tables
    """

    @classmethod
    @LoggingUtil.logging_to_appins
    def delete_table(cls, db_name: str, table_name: str):
        """
        Delete the given table in the given database. This should be used for tables that do not use
        delta as the underlying storag mechanism

        **IMPORTANT**

        Delta recommends that if you want to only delete the content of a table, then not to
        delete the table itself as this will remove the history. Please use `delete_table_contents` if this is what
        you wish to do

        :param db_name: Name of the database the table belongs to
        :param table_name: The name of the table to delete
        """
        spark = SparkSession.builder.getOrCreate()
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            table_details_query = spark.sql(f"DESCRIBE DETAIL {db_name}.{table_name}")
            num_tables = table_details_query.count()
            if num_tables > 1:
                raise RuntimeError("too many locations associated with the table!")
            else:
                loc = table_details_query.select("location").first().location
                mssparkutils.fs.rm(loc, True)
                spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
                LoggingUtil().log_info(f"Dropped table {db_name}.{table_name}")
        else:
            LoggingUtil().log_info("Table does not exist")

    @classmethod
    @LoggingUtil.logging_to_appins
    def delete_table_contents(cls, db_name: str, table_name: str):
        """
        Delete the content from the given table in the given database. This should be used for
        tables that use delta format as the underlying storage mechanism

        :param db_name: Name of the database the table belongs to
        :param table_name: The name of the table to delete
        """
        spark = SparkSession.builder.getOrCreate()
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            table_details_query = spark.sql(f"DESCRIBE DETAIL {db_name}.{table_name}")
            num_tables = table_details_query.count()
            if num_tables > 1:
                raise RuntimeError("too many locations associated with the table!")
            else:
                spark.sql(f"DELETE FROM {db_name}.{table_name}")
                LoggingUtil().log_info(f"Deleted the content from table {db_name}.{table_name}")
        else:
            LoggingUtil().log_info("Table does not exist")


### Module odw.core.io.synapse_data_io

#COMMENTOUT from odw.core.io.data_io import DataIO
from pyspark.sql import DataFrame


class SynapseDataIO(DataIO):
    """
    Manages data io to/from a storage location that is linked to Synapse
    """
    def _format_to_adls_path(self, container_name: str, blob_path: str, storage_name: str = None, storage_endpoint: str = None) -> str:
        """
        Return a datalake path from the given arguments.

        **Note this function exists so that the filepath can be mocked during testing**
        
        :param str container_name: The storage container the blob exists in
        :param str blob_path: The path to the blob in the container
        :param str storage_name: The name of the storage account
        :param str storage_endpoint: The endpoint of the storage account, of the form `mystorageaccount.dfs.core.windows.net/`
        :return str: A string with the format `abfss://{container_name}@{storage_name}.dfs.core.windows.net/{blob_path}`
        """
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseDataIO._format_to_adls_path expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseDataIO._format_to_adls_path expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if storage_name:
            storage_endpoint = f"{storage_name}.dfs.core.windows.net/"
        storage_endpoint_split = storage_endpoint.split(".")
        if len(storage_endpoint_split) != 5:
            raise ValueError(
                f"The storage endpoint '{storage_endpoint}' generated by SynapseDataIO._format_to_adls_path does not "
                "conform to the format 'name.kind.core.windows.net/'"
            )
        if not storage_endpoint.endswith("/"):
            storage_endpoint += "/"
        return f"abfss://{container_name}@{storage_endpoint}{blob_path}"

    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame

        :param str storage_name: The name of the storage account to read from
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        :param SparkSession spark: The spark session
        
        :return DataFrame: The data 
        """
        pass
    
    def write(self, data: DataFrame, **kwargs):
        """
        Write the data to the given storage location
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param SparkSession spark: The spark session
        """
        pass


### Module odw.core.io.synapse_delta_io

#COMMENTOUT from odw.core.io.synapse_data_io import SynapseDataIO
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable


class SynapseDeltaIO(SynapseDataIO):
    """
    Manages delta file data io to/from an Azure Data Lake Storage Gen 2 account that is linked to Synapse

    # Example usage
    ## Reading
    ```
    data_frame = SynapseDeltaIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark
    )
    ```
    ## Writing

    ```
    SynapseDeltaIO().write(
        data_frame,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
    )
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        return "ADLSG2-Delta"

    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given delta table, and return the data as a pyspark DataFrame

        :param str storage_name: The name of the storage account to read from. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to read from. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        :param str file_format: The file format to read
        :param SparkSession spark: The spark session
        
        :return DataFrame: The data 
        """
        spark: SparkSession = kwargs.get("spark", None)
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        if not spark:
            raise ValueError(f"SynapseDeltaIO.read requires a spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseDeltaIO.read expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseDeltaIO.read expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseDeltaIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseDeltaIO.read requires a blob_path to be provided, but was missing")
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        return DeltaTable.forPath(spark, data_path).toDF()

    def write(self, data: DataFrame, **kwargs):
        """
        Add the data as a new entry to a delta table.
        
        The data **MUST** have a `update_key_col` column with values `{"create", "update", "delete"}` reflecting if a row was 
        created, updated or deleted
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param str merge_keys: A list of primary keys in the data
        :param str update_key_col: The column used to determine if a row is updated, created or deleted
        """
        spark: SparkSession = kwargs.get("spark", None)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        merge_keys = kwargs.get("merge_keys", None)
        update_key_col = kwargs.get("update_key_col", None)
        if not spark:
            raise ValueError(f"SynapseDeltaIO.read requires spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseDeltaIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseDeltaIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseDeltaIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseDeltaIO.write requires a blob_path to be provided, but was missing")
        if bool(database_name) ^ bool(table_name):
            raise ValueError(f"SynapseTableDataIO.write requires both database_name and table_name to be provided, or neither")
        if not merge_keys:
            raise ValueError(f"SynapseDeltaIO.write requires a merge_keys to be provided, but was missing")
        if not update_key_col:
            raise ValueError(f"SynapseDeltaIO.write requires a update_key_col to be provided, but was missing")
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        target_delta_table = DeltaTable.forPath(spark, data_path)
        delta_table_schema = target_delta_table.toDF().schema
        delta_table_cols = set(delta_table_schema.names)
        new_data_cols = set(data.schema.names)
        if not (all(x in delta_table_cols for x in merge_keys) and all(x in new_data_cols for x in merge_keys)):
            missing_in_delta = [x for x in merge_keys if x not in delta_table_cols]
            missing_in_new_data = [x for x in merge_keys if x not in new_data_cols]
            raise ValueError(
                f"Not all specified merge_keys are in the target or new data.\n"
                f"Of the following primary keys {merge_keys}\n"
                f"The primary keys {missing_in_delta} were missing in the target delta table which has columns {delta_table_cols}\n"
                f"The primary keys {missing_in_new_data} were missing in the new data which has columns {new_data_cols}"
            )
        if update_key_col not in new_data_cols:
            raise ValueError(f"The data is expected to have a '{update_key_col}' column")
        create_strings = ("create", "created")
        update_strings = ("update", "updated")
        delete_strings = ("delete", "deleted")
        all_search_strings = create_strings + update_strings + delete_strings
        # Convert update_key_col to lower case for consistency
        data = data.withColumn(update_key_col, F.lower(F.col(update_key_col)))
        # Ensure all values in the update_key_col are valid before proceeding
        invalid_update_key_rows = data.filter(~F.col(update_key_col).isin(*all_search_strings))
        if invalid_update_key_rows.count() > 0:
            raise ValueError(f"Some of the rows in the data's '{update_key_col}' column do not match one of '{all_search_strings}'")
        target_delta_table.alias("t").merge(
            data.alias("s"),
            " AND ".join(f"t.{key} = s.{key}" for key in merge_keys)
        ).whenMatchedUpdate(  # Update existing records
            condition=f"s.{update_key_col} IN {update_strings}",
            set = {
                col_name: F.expr(f"s.{col_name}")
                for col_name in data.schema.names
                if col_name != update_key_col
            },
        ).whenMatchedDelete(  # Delete records
            condition=f"s.{update_key_col} IN {delete_strings}"
        ).whenNotMatchedInsert(  # Insert new records
            condition=f"s.{update_key_col} IN {create_strings}",
            values={
                col_name: F.expr(f"s.{col_name}")
                for col_name in data.schema.names
                if col_name != update_key_col
            }
        ).execute()
        if database_name:
            # Create a table out of the delta file if table details are specified
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING DELTA
                LOCATION '{data_path}'
            """)


### Module odw.core.io.synapse_file_data_io

#COMMENTOUT from odw.core.io.synapse_data_io import SynapseDataIO
from pyspark.sql import DataFrame, SparkSession


class SynapseFileDataIO(SynapseDataIO):
    """
    Manages file data io to/from an Azure Data Lake Storage Gen 2 account that is linked to Synapse

    # Example usage
    ## Reading
    ```
    data_frame = SynapseFileDataIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        file_format="parquet",
        spark=spark
    )
    ```
    ## Writing

    ```
    SynapseFileDataIO().write(
        data_frame,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        file_format="parquet",
        write_mode="overwrite"
    )
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        return "ADLSG2-File"

    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame

        :param str storage_name: The name of the storage account to read from. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to read from. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        :param str file_format: The file format to read
        :param SparkSession spark: The spark session
        
        :return DataFrame: The data 
        """
        spark: SparkSession = kwargs.get("spark", None)
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        file_format = kwargs.get("file_format", None)
        read_options = kwargs.get("read_options", dict())
        if not spark:
            raise ValueError(f"SynapseFileDataIO.read requires a spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseFileDataIO.read expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseFileDataIO.read expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseFileDataIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseFileDataIO.read requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError(f"SynapseFileDataIO.read requires a file_format to be provided, but was missing")
        if not isinstance(read_options, dict):
            raise ValueError(f"SynapseFileDataIO.read requires the read_options to be a list of strings, but was a {type(read_options)}")
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        reader = spark.read.format(file_format)
        for option_name, option_value in read_options.items():
            reader.option(option_name, option_value)
        return reader.load(data_path)

    def write(self, data: DataFrame, **kwargs):
        """
        Write the data to the given storage location
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param str file_format: The file format to write
        :param str write_mode: The pyspark write mode
        """
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        file_format = kwargs.get("file_format", None)
        write_mode = kwargs.get("write_mode", None)
        write_options = kwargs.get("write_options", dict())
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseFileDataIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseFileDataIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseFileDataIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseFileDataIO.write requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError(f"SynapseDeltaDataIO.write requires a file_format to be provided, but was missing")
        if not write_mode:
            raise ValueError(f"SynapseDeltaDataIO.write requires a write_mode to be provided, but was missing")
        if not isinstance(write_options, dict):
            raise ValueError(f"SynapseFileDataIO.write requires the write_options to be a list of strings, but was a {type(write_options)}")
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        writer = data.write.format(file_format).mode(write_mode)
        for option_name, option_value in write_options.items():
            writer.option(option_name, option_value)
        writer.save(data_path)


### Module odw.core.io.synapse_table_data_io

#COMMENTOUT from odw.core.io.synapse_data_io import SynapseDataIO
from pyspark.sql import DataFrame, SparkSession


class SynapseTableDataIO(SynapseDataIO):
    """
    Manages table data io to/from an Azure Data Lake Storage Gen 2 account that is linked to Synapse

    # Example usage
    ## Reading
    ```
    data_frame = SynapseTableDataIO().read(
        storage_name="mystorageaccount",
        database_name="my_db",
        table_name="my_table",
        file_format="parquet",
        spark=spark
    )
    ```
    ## Writing

    ```
    SynapseTableDataIO().write(
        data_frame,
        database_name="my_db",
        table_name="my_table",
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        file_format="parquet",
        write_mode="overwrite"
    )
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        return "ADLSG2-Table"

    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame

        :param str database_name: The name of the database to read the data from
        :param str table_name: The name of the table to read (from the given database)
        :param str file_format: The underlying file format of the table to read
        :param SparkSession spark: The spark session

        :return DataFrame: The data
        """
        spark: SparkSession = kwargs.get("spark", None)
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        file_format = kwargs.get("file_format", None)
        if not spark:
            raise ValueError(f"SynapseTableDataIO.read requires a spark to be provided, but was missing")
        if not database_name:
            raise ValueError(f"SynapseTableDataIO.read requires a database_name to be provided, but was missing")
        if not table_name:
            raise ValueError(f"SynapseTableDataIO.read requires a table_name to be provided, but was missing")
        if not file_format:
            raise ValueError(f"SynapseTableDataIO.read requires a file_format to be provided, but was missing")
        table_path = f"{database_name}.{table_name}"
        return spark.read.format(file_format).table(table_path)

    def write(self, data: DataFrame, **kwargs):
        """
        Write the data to the given storage location

        :param DataFrame data: The data to write
        :param str database_name: The name of the database to write the data to
        :param str table_name: The name of the table to write (to the given database)
        :param str storage_name: The name of the storage account to write the underlying data to. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to write the underlying data to. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to write the underlying data to
        :param str blob_path: The path to the blob (in the container) to write the underlying data to
        :param str file_format: The underlying file format of the table to write
        :param str write_mode: The pyspark write mode for writing the underlying data
        :param list[tuple[str, str]] write_options: Additional spark options when writing the data
        """
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        file_format = kwargs.get("file_format", None)
        write_mode = kwargs.get("write_mode", None)
        write_options = kwargs.get("write_options", [])
        if not database_name:
            raise ValueError(f"SynapseTableDataIO.write requires a database_name to be provided, but was missing")
        if not table_name:
            raise ValueError(f"SynapseTableDataIO.write requires a table_name to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseTableDataIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseTableDataIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseTableDataIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseTableDataIO.write requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError(f"SynapseTableDataIO.write requires a file_format to be provided, but was missing")
        if not write_mode:
            raise ValueError(f"SynapseDeltaDataIO.write requires a write_mode to be provided, but was missing")
        table_path = f"{database_name}.{table_name}"
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        print("data_path: ", data_path)
        write_options = set([("path", data_path)] + write_options)
        writer = data.write.format(file_format).mode(write_mode)
        for option_name, option_value in write_options:
            writer.option(option_name, option_value)
        writer.saveAsTable(table_path)


### Module odw.core.io.synapse_legacy_delta_io

#COMMENTOUT from odw.core.io.synapse_delta_io import SynapseDeltaIO
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable


class SynapseLegacyDeltaIO(SynapseDeltaIO):
    """
    Manages delta file data io to/from an Azure Data Lake Storage Gen 2 account that is linked to Synapse

    This is a legacy process that writes "fake" delta files using an incremental key, which results in the
    history of the data being preserved

    # Example usage
    ## Reading
    ```
    data_frame = SynapseLegacyDeltaIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark
    )
    ```
    ## Writing

    ```
    SynapseLegacyDeltaIO().write(
        data_frame,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
    )
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        return "ADLSG2-LegacyDelta"

    def write(self, data: DataFrame, **kwargs):
        """
        Add the data as a new entry to a delta table.
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str storage_endpoint: The endpoint of the storage account to write to. Expects either storage_name or storage_endpoint but not both
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param str merge_keys: A list of primary keys in the data
        """
        spark: SparkSession = kwargs.get("spark", None)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        merge_keys = kwargs.get("merge_keys", None)
        if not spark:
            raise ValueError(f"SynapseLegacyDeltaIO.read requires spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError(f"SynapseLegacyDeltaIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError(f"SynapseLegacyDeltaIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError(f"SynapseLegacyDeltaIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseLegacyDeltaIO.write requires a blob_path to be provided, but was missing")
        if bool(database_name) ^ bool(table_name):
            raise ValueError(f"SynapseTableDataIO.write requires both database_name and table_name to be provided, or neither")
        if not merge_keys:
            raise ValueError(f"SynapseLegacyDeltaIO.write requires a merge_keys to be provided, but was missing")
        if storage_name:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_name=storage_name)
        else:
            data_path = self._format_to_adls_path(container_name, blob_path, storage_endpoint=storage_endpoint)
        target_delta_table = DeltaTable.forPath(spark, data_path)
        delta_table_schema = target_delta_table.toDF().schema
        delta_table_cols = set(delta_table_schema.names)
        new_data_cols = set(data.schema.names)
        if not (all(x in delta_table_cols for x in merge_keys) and all(x in new_data_cols for x in merge_keys)):
            missing_in_delta = [x for x in merge_keys if x not in delta_table_cols]
            missing_in_new_data = [x for x in merge_keys if x not in new_data_cols]
            raise ValueError(
                f"Not all specified merge_keys are in the target or new data.\n"
                f"Of the following primary keys {merge_keys}\n"
                f"The primary keys {missing_in_delta} were missing in the target delta table which has columns {delta_table_cols}\n"
                f"The primary keys {missing_in_new_data} were missing in the new data which has columns {new_data_cols}"
            )
        target_delta_table.alias("t").merge(
            data.alias("s"),
            " AND ".join(f"t.{key} = s.{key}" for key in merge_keys)
        ).whenMatchedUpdate(  # Update existing records
            set = {
                col_name: F.expr(f"s.{col_name}")
                for col_name in data.schema.names
            },
        ).whenNotMatchedInsert(  # Insert new records
            values={
                col_name: F.expr(f"s.{col_name}")
                for col_name in data.schema.names
            }
        ).execute()
        if database_name:
            # Create a table out of the delta file if table details are specified
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING DELTA
                LOCATION '{data_path}'
            """)


### Module odw.core.io.data_io_factory

#COMMENTOUT from odw.core.io.data_io import DataIO
#COMMENTOUT from odw.core.exceptions import DuplicateDataIONameException, DataIONameNotFoundException
#COMMENTOUT from odw.core.io.data_io import DataIO
#COMMENTOUT from odw.core.io.synapse_file_data_io import SynapseFileDataIO
#COMMENTOUT from odw.core.io.synapse_table_data_io import SynapseTableDataIO
##COMMENTOUT from odw.core.io.azure_blob_data_io import AzureBlobDataIO
#COMMENTOUT from odw.core.io.synapse_delta_io import SynapseDeltaIO
#COMMENTOUT from odw.core.io.synapse_legacy_delta_io import SynapseLegacyDeltaIO
from typing import Set, List, Dict, Type
import json


class DataIOFactory():
    DATA_IO_CLASSES: Set[Type[DataIO]] = {
        SynapseFileDataIO,
        SynapseTableDataIO,
        #AzureBlobDataIO,
        SynapseDeltaIO,
        SynapseLegacyDeltaIO
    }

    @classmethod
    def _validate_data_io_classes(cls):
        name_map: Dict[str, List[Type[DataIO]]] = dict()
        for data_io_class in cls.DATA_IO_CLASSES:
            type_name = data_io_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(data_io_class)
            else:
                name_map[type_name] = [data_io_class]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateDataIONameException(
                f"The following DataIO implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, data_io_name: str) -> Type[DataIO]:
        data_io_map = cls._validate_data_io_classes()
        if data_io_name not in data_io_map:
            raise DataIONameNotFoundException(
                f"No DataIO class could be found for dataio name '{data_io_name}'"
            )
        return data_io_map[data_io_name]


### Module odw.core.etl.etl_process

#COMMENTOUT from odw.core.io.data_io_factory import DataIOFactory
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
#COMMENTOUT from odw.core.etl.etl_result import ETLResult, ETLFailResult
#COMMENTOUT from odw.core.io.synapse_file_data_io import SynapseFileDataIO
#COMMENTOUT from odw.core.util.util import Util
from typing import List, Dict, Any, Tuple
from notebookutils import mssparkutils
import traceback
from datetime import datetime
import json


class ETLProcess(ABC):
    def __init__(self, spark: SparkSession, debug: bool = False):
        self.spark = spark
        self.debug = debug

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        Return a unique name for the ETL process, to be identified as part of the factory

        :return str: A unique name for the process
        """

    @classmethod
    def load_parameter(cls, param_name: str, kwargs: Dict[str, Any], default: Any = None):
        param_value = kwargs.get(param_name, default)
        if param_value is None:
            raise ValueError(f"{cls.__name__} requires a {param_name} parameter to be provided, but was missing")
        return param_value

    @classmethod
    @LoggingUtil.logging_to_appins
    def get_all_files_in_directory(cls, source_path: str):
        files_to_explore = set(mssparkutils.fs.ls(source_path))
        found_files = set()
        while files_to_explore:
            next_item = files_to_explore.pop()
            if next_item.isDir:
                files_to_explore |= set(mssparkutils.fs.ls(next_item.path))
            else:
                found_files.add(next_item.path)
        return found_files
    
    @classmethod
    def load_orchestration_data(self):
        """
        Load the orchestration file
        """
        return SynapseFileDataIO().read(
            spark=SparkSession.builder.getOrCreate(),
            storage_endpoint=Util.get_storage_account(),
            container_name="odw-config",
            blob_path=f"orchestration/orchestration.json",
            file_format="json",
            read_options={"multiline": "true"},
        )

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data using the given kwargs

        :param List[Dict[str, Any]] data_to_read: Data to read. List of entries that can each be fed into a relevant DataIO class
        :return Dict[str, DataFrame]: A dictionary of table names mapped to the underlying data
        """
        data_to_read: List[Dict[str, Any]] = kwargs.get("data_to_read", None)
        if not data_to_read:
            raise ValueError(f"ETLProcess expected a data_to_read parameter to be passed, but this was missing")
        data_map = dict()
        for metadata in data_to_read:
            data_name = metadata.get("data_name", None)
            storage_kind = metadata.get("storage_kind", None)
            data_format = metadata.get("data_format", None)
            if not data_name:
                raise ValueError(f"ETLProcess data_to_read expected a data_name parameter to be passed, but this was missing")
            if not storage_kind:
                raise ValueError(f"ETLProcess data_to_read expected a storage_kind parameter to be passed, but this was missing")
            if not data_format:
                raise ValueError(f"ETLProcess data_to_read expected a data_format parameter to be passed, but this was missing")
            data_io_inst = DataIOFactory.get(storage_kind)()
            data = data_io_inst.read(**metadata)
            data_map[data_name] = data
        return data_map

    @abstractmethod
    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Perform transformations on the given input data
        """
        pass

    def write_data(self, data_to_write: Dict[str, Any]):
        """
        Write the given dictionary of tables

        :param Dict[str, Any] data_to_write: A dictionary of the form <table_name, table_metadata>
                                             which can be fed into a relevant DataIO class
        """
        for table_name, table_metadata in data_to_write.items():
            storage_kind = table_metadata.get("storage_kind", None)
            if not storage_kind:
                raise ValueError(f"ETLProcess expected a storage_kind parameter to be passed, but this was missing")
            data_io_inst = DataIOFactory.get(storage_kind)()
            data_io_inst.write(**table_metadata, spark=self.spark)

    def _log_data_to_write(self, data_to_write: Dict[str, Any]):
        if not self.debug:
            data_to_write_cleaned = {
                k: {
                    subk: subv for subk, subv in v.items() if not isinstance(v, DataFrame)
                }
                for k, v in data_to_write.items()
            }
            LoggingUtil().log_info(f"The following data will be written: {json.dumps(data_to_write_cleaned, indent=4, default=str)}")
            return
        for table_name, table_metadata in data_to_write.items():
            table_metadata_cleaned = {k: v for k, v in table_metadata.items() if not isinstance(v, DataFrame)}
            LoggingUtil().log_info(f"Metadata for the write entry '{table_name}': {json.dumps(table_metadata_cleaned, indent=4, default=str)}")
            data = table_metadata.get("data", None)
            if data:
                if isinstance(data, DataFrame):
                    Util.display_dataframe(data)
                if isinstance(data, dict):
                    print(json.dumps(data, indent=4, default=str))
                else:
                    LoggingUtil().log_info(f"Could not display the data. It is of unexpected type '{type(data)}'")
            else:
                LoggingUtil().log_info(f"There is no data to write")

    def run(self, **kwargs) -> ETLResult:
        """
        Run the full ETL process with the given arguments

        Steps
        1. Load source data
        2. Perform transformations on the loaded data
        3. Write the transformed data

        If there is an exception detected at any point in the process, an ETLFailResult is
        returned containing the error trace. Otherwise an ETLSuccessResult is returned

        :param kwargs: Any arguments as specified in the concrete implementation classes
        :returns: An ETLResult. Exceptions are caught internally and are returned in the `metadata.exception` property of an ETLFailResult

        """
        etl_start_time = datetime.now()

        def generate_failure_result(start_time: datetime, exception: str, exception_trace=None, table_name=None):
            end_time = datetime.now()
            return ETLFailResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=start_time,
                    end_execution_time=end_time,
                    exception=exception,
                    exception_trace=exception_trace,
                    table_name=table_name,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    insert_count=0,
                    update_count=0,
                    delete_count=0,
                )
            )

        try:
            source_data_map = self.load_data(**kwargs)
            LoggingUtil().log_info(f"The following tables were loaded: {json.dumps(list(source_data_map), indent=4)}")
            data_to_write, etl_result = self.process(source_data=source_data_map, **kwargs)
        except Exception as e:
            failure_result = generate_failure_result(etl_start_time, str(e), traceback.format_exc())
            LoggingUtil().log_error(failure_result)
            return failure_result
        if isinstance(etl_result, ETLFailResult):
            LoggingUtil().log_error(etl_result)
            return etl_result
        self._log_data_to_write(data_to_write)
        try:
            self.write_data(data_to_write)
            LoggingUtil().log_info(etl_result)
            return etl_result
        except Exception as e:
            failure_result = generate_failure_result(etl_start_time, str(e), traceback.format_exc(), table_name=", ".join(data_to_write.keys()))
            LoggingUtil().log_error(failure_result)
            return failure_result


### Module odw.core.etl.transformation.transformation_process

#COMMENTOUT from odw.core.etl.etl_process import ETLProcess
from typing import Dict, List, Any


class TransformationProcess(ETLProcess):
    pass


### Module odw.core.etl.transformation.harmonised.harmonsation_process

#COMMENTOUT from odw.core.etl.transformation.transformation_process import TransformationProcess


class HarmonisationProcess(TransformationProcess):
    pass


### Module odw.core.etl.transformation.standardised.standardisation_process

#COMMENTOUT from odw.core.etl.transformation.transformation_process import TransformationProcess
#COMMENTOUT from odw.core.util.util import Util
#COMMENTOUT from odw.core.util.azure_blob_util import AzureBlobUtil
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
#COMMENTOUT from odw.core.util.table_util import TableUtil
#COMMENTOUT from odw.core.etl.etl_result import ETLResult, ETLResultFactory
#COMMENTOUT from odw.core.etl.util.schema_util import SchemaUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from datetime import datetime, timedelta, date
from typing import Dict, List, Any
import json
import re
from copy import deepcopy


class StandardisationProcess(TransformationProcess):
    @classmethod
    def get_name(cls):
        return "Standardisation"

    def standardise(
        self,
        data: DataFrame,
        schema: Dict[str, Any],
        expected_from: datetime,
        expected_to: datetime,
        process_name: str,
        definition: Dict[str, Any]
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
            "file_ID": F.sha2(F.concat(F.lit(F.input_file_name()), F.current_timestamp().cast("string")), 256)
        }
        for col_name, col_value in col_value_map.items():
            df = df.withColumn(col_name, col_value)

        ### change any array field to string
        standardised_table_schema = deepcopy(schema)
        for field in standardised_table_schema['fields']:
            if field["type"] == "array":
                field["type"] = "string"
        standardised_table_schema = StructType.fromJson(standardised_table_schema)

        ### remove characters that Delta can't allow in headers and add numbers to repeated column headers
        cols_orig = df.schema.names
        cols=[re.sub('[^0-9a-zA-Z]+', '_', i).lower() for i in cols_orig]
        cols=[colm.rstrip('_') for colm in cols]
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
            raise ValueError(f"StandardisationProcess.process requires a source_data dictionary to be provided, but was missing")
        orchestration_file: Dict[str, Any] = kwargs.get("orchestration_file", None)
        orchestration_file = deepcopy(orchestration_file)
        if not orchestration_file:
            raise ValueError(f"StandardisationProcess.process requires a orchestration_file json to be provided, but was missing")
        date_folder_input: str = kwargs.get("date_folder", None)
        source_frequency_folder: str = kwargs.get("source_frequency_folder")
        specific_file: str = kwargs.get("specific_file", None) # if not provided, it will ingest all files in the date_folder
        # Initialise variables
        if date_folder_input == '':
            date_folder = datetime.now().date()
        else:
            date_folder = datetime.strptime(date_folder_input, "%Y-%m-%d")
        spark = SparkSession.builder.getOrCreate()
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
                    if (specific_file == "" or d["Source_Filename_Start"] == specific_file) and
                       (not source_frequency_folder or d["Source_Frequency_Folder"] == source_frequency_folder) and
                       file.startswith(d["Source_Filename_Start"]
                    )
                ),
                None
            )
            expected_from = date_folder - timedelta(days=1)
            expected_from = datetime.combine(expected_from, datetime.min.time())
            expected_to = expected_from + timedelta(days=definition["Expected_Within_Weekdays"])
            if "Standardised_Table_Definition" in definition:
                standardised_table_loc = Util.get_path_to_file(f"odw-config/{definition['Standardised_Table_Definition']}")
                standardised_table_schema = json.loads(spark.read.text(standardised_table_loc, wholetext=True).first().value)
            else:
                standardised_table_schema = SchemaUtil(db_name="odw_standardised_db").get_schema_for_entity(definition["Source_Frequency_Folder"])
            df = self.standardise(data, standardised_table_schema, expected_from, expected_to, process_name, definition)
            output_data[file] = df
        return output_data

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        data_to_read: List[Dict[str, Any]] = kwargs.get("data_to_read", None)
        if not data_to_read:
            raise ValueError(f"StandardisationProcess expected a data_to_read parameter to be passed, but this was missing")
        data_map = super().load_data(data_to_read)
        # Fetch the contents of the orchestration file
        storage_account = Util.get_storage_account()
        orchestration_file_bytes = AzureBlobUtil(storage_endpoint=storage_account).read("odw-config", "orchestration/orchestration.json")
        orchestration_file = json.load(orchestration_file_bytes)
        data_map["orchestration_file"] = orchestration_file
        return data_map


### Module odw.core.etl.transformation.harmonised.service_bus_harmonisation_process

#COMMENTOUT from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
#COMMENTOUT from odw.core.util.util import Util
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
#COMMENTOUT from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
#COMMENTOUT from odw.core.io.synapse_table_data_io import SynapseTableDataIO
#COMMENTOUT from odw.core.util.udf import absolute
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.errors.exceptions.captured import AnalysisException
from datetime import datetime
from typing import Dict
import json


class ServiceBusHarmonisationProcess(HarmonisationProcess):
    """
    # Example usage

    ```
    params = {
    "entity_stage_name": "Service Bus Harmonisation",
    "entity_name": "appeal-has"
}
    HorizonStandardisationProcess(spark).run(**params)
    ```
    """
    def __init__(self, spark):
        super().__init__(spark)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"

    @classmethod
    def get_name(cls):
        return "Service Bus Harmonisation"

    def load_data(self, **kwargs):
        entity_name = self.load_parameter("entity_name", kwargs)
        # Load orchestration data
        orchestration_data = self.load_orchestration_data()

        definitions: list = json.loads(orchestration_data.toJSON().first())["definitions"]
        definition: dict = next((d for d in definitions if entity_name == d["Source_Filename_Start"]), None)
        if not definition:
            raise RuntimeError(
                f"No definition could be found for 'Source_Filename_Start' == '{entity_name}' in the followin definitions: "
                f"{json.dumps(definitions, indent=4)}"
            )
        std_table: str = definition["Standardised_Table_Name"]
        hrm_table: str = definition["Harmonised_Table_Name"]
        standardised_table_path = f"{self.std_db}.{std_table}"
        harmonised_table_path = f"{self.hrm_db}.{hrm_table}"

        # Load new records from the standardised layer
        # Note this does not work if the harmonised table does not exist at the start - fix after first int test is working
        new_records = self.spark.sql(
            f"SELECT * FROM {standardised_table_path} WHERE message_id not in (SELECT DISTINCT message_id FROM {harmonised_table_path} where message_id IS NOT NULL) ORDER BY message_enqueued_time_utc"
        )
        # Load the harmonised layer
        try:
            existing_harmonised_data = SynapseTableDataIO().read(
                spark=self.spark, database_name=self.hrm_db, table_name=hrm_table, file_format="delta"
            )
        except AnalysisException:
            existing_harmonised_data = None

        # Extract source system data
        LoggingUtil().log_info(f"Attempting to load '{self.hrm_db}.main_sourcesystem_fact'")
        source_system_data: DataFrame = self.spark.sql(
            f"SELECT * FROM {self.hrm_db}.main_sourcesystem_fact WHERE Description = 'Casework' AND IsActive = 'Y'"
        )
        return {
            "orchestration_data": orchestration_data,
            "new_data": new_records,
            "existing_data": existing_harmonised_data,
            "source_system_data": source_system_data,
        }

    def harmonise(self, data: DataFrame, source_system_data: DataFrame, incremental_key: str, primary_key: str):
        """
        Add harmonised columns to the dataframe, and drop unnecessary columns
        """
        data = data.drop("ingested_datetime")
        source_system_id = source_system_data.select("SourceSystemID").collect()[0][0]
        cols_to_add = {
            "SourceSystemID": F.lit(source_system_id),
            "RowID": F.lit("").cast("string"),
            "migrated": F.lit("1").cast("string"),
            "ODTSourceSystem": F.lit("ODT").cast("string"),
            "ValidTo": F.lit("").cast("string"),
            "IsActive": F.lit("Y").cast("string"),
            incremental_key: absolute(
                F.hash(
                    F.col(primary_key).cast(T.StringType()),
                    F.col("message_enqueued_time_utc").cast(T.StringType())
                )
            ).cast(T.LongType()),
            "IngestionDate": F.col("message_enqueued_time_utc").cast("string"),
        }
        for col_name, col_value in cols_to_add.items():
            data = data.withColumn(col_name, col_value)
        cols_to_drop = ("ingested_datetime", "message_enqueued_time_utc", "expected_from", "expected_to", "input_file")
        for col_to_drop in cols_to_drop:
            data = data.drop(col_to_drop)
        data = data.dropDuplicates()
        return data

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        # Load parameters
        entity_name: str = self.load_parameter("entity_name", kwargs)
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        orchestration_data: DataFrame = self.load_parameter("orchestration_data", source_data)
        new_data: DataFrame = self.load_parameter("new_data", source_data)
        existing_data: DataFrame = self.load_parameter("existing_data", source_data)
        source_system_data: DataFrame = self.load_parameter("source_system_data", source_data)

        definitions: list = json.loads(orchestration_data.toJSON().first())["definitions"]
        definition: dict = next((d for d in definitions if entity_name == d["Source_Filename_Start"]), None)
        if not definition:
            raise RuntimeError(
                f"No definition could be found for 'Source_Filename_Start' == '{entity_name}' in the followin definitions: "
                f"{json.dumps(definitions, indent=4)}"
            )
        std_table: str = definition["Standardised_Table_Name"]
        hrm_table: str = definition["Harmonised_Table_Name"]
        hrm_incremental_key: str = definition["Harmonised_Incremental_Key"]
        entity_primary_key: str = definition["Entity_Primary_Key"]
        standardised_table_path = f"{self.std_db}.{std_table}"
        harmonised_table_path = f"{self.hrm_db}.{hrm_table}"

        new_data = self.harmonise(new_data, source_system_data, hrm_incremental_key, entity_primary_key)
        # Note this does not work if the harmonised table does not exist at the start - fix after first int test is working
        # new_data = new_data.select(existing_data.columns)

        new_data = new_data.withColumn(
            "row_state_metadata",
            F.when(F.col("message_type") == "Create", F.lit("create"))
            .when(F.col("message_type").isin(["update", "Update", "Publish", "Unpublish"]), F.lit("update"))
            .otherwise(F.lit("delete")),
        )
        new_data = new_data.drop("message_type")
        # The legacy process keeps a history of rows - insert the old rows with IsActive=False to work with the new delta write
        joined_updated_rows = existing_data.join(new_data, new_data[entity_primary_key] == existing_data[entity_primary_key], "left").filter(
            (new_data["row_state_metadata"] == "update") | (new_data["row_state_metadata"] == "delete")
        ).drop("row_state_metadata")
        update_rows = joined_updated_rows
        # Update to match new schema
        for col in new_data.columns:
            if col in existing_data.columns:
                update_rows = update_rows.withColumn(f"{col}__cleaned", existing_data[col])
            elif col != "row_state_metadata":
                update_rows = update_rows.withColumn(f"{col}__cleaned", F.lit(None).cast(update_rows.schema[col].dataType))
        update_rows = update_rows.withColumn("ValidTo__cleaned", new_data["IngestionDate"]).withColumn("IsActive__cleaned", F.lit("N"))
        cols_to_keep = [col for col in update_rows.columns if col.endswith("__cleaned")]
        update_rows = update_rows.select(cols_to_keep)
        for col in cols_to_keep:
            col_cleaned = col[:-9]
            update_rows = update_rows.withColumnRenamed(col, col_cleaned)
        new_data =new_data.filter(F.col("row_state_metadata") != "delete").drop("row_state_metadata")
        new_data = new_data.union(update_rows).dropDuplicates()

        hrm_table_snake_case = hrm_table.replace("-", "_")

        data_to_write = {
            harmonised_table_path: {
                "data": new_data,
                "storage_kind": "ADLSG2-LegacyDelta",
                "database_name": "odw_harmonised_db",
                "table_name": hrm_table,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": f"{hrm_table_snake_case}",
                "merge_keys": [entity_primary_key, hrm_incremental_key],
                "update_key_col": "row_state_metadata",
            }
        }

        # IngestionFunctions(self.spark).compare_and_merge_schema(new_data, harmonised_table_path)
        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=harmonised_table_path,
                insert_count=new_data.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )


### Module odw.core.etl.transformation.standardised.horizon_standardisation_process

#COMMENTOUT from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
#COMMENTOUT from odw.core.util.util import Util
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
#COMMENTOUT from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
#COMMENTOUT from odw.core.io.synapse_table_data_io import SynapseTableDataIO
#COMMENTOUT from odw.core.io.synapse_file_data_io import SynapseFileDataIO
#COMMENTOUT from odw.core.etl.util.schema_util import SchemaUtil
#COMMENTOUT from odw.core.anonymisation import AnonymisationEngine, load_config
from notebookutils import mssparkutils
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException
from datetime import datetime, timedelta
from typing import Dict, List, Any
import re
import json
import os
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

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.anonymisation_engine = None
        self._init_anonymisation()

    def _init_anonymisation(self):
        """Initialize anonymisation engine if enabled via environment variables."""
        enable_anon = os.environ.get("ODW_ENABLE_ANONYMISATION", "false").lower() == "true"
        if enable_anon:
            try:
                config_path = os.environ.get("ODW_ANONYMISATION_CONFIG_PATH")
                if config_path:
                    config = load_config(config_path)
                    self.anonymisation_engine = AnonymisationEngine(config)
                else:
                    self.anonymisation_engine = AnonymisationEngine()
                LoggingUtil().log_info("[HorizonStandardisationProcess] Anonymisation engine initialized")
            except Exception as e:
                LoggingUtil().log_error(f"[HorizonStandardisationProcess] Failed to initialize anonymisation: {e}")
                self.anonymisation_engine = None
        else:
            LoggingUtil().log_info("[HorizonStandardisationProcess] Anonymisation disabled")

    def apply_anonymisation(self, df: DataFrame, entity_name: str, source_folder: str) -> DataFrame:
        """
        Apply anonymisation if the engine is initialized.

        Args:
            df: The DataFrame to anonymise
            entity_name: The entity name for Purview lookup
            source_folder: The source folder (e.g., 'Horizon')

        Returns:
            Anonymised DataFrame if enabled, otherwise returns the original DataFrame
        """
        if self.anonymisation_engine is None:
            return df

        try:
            anonymised_df = self.anonymisation_engine.apply_from_purview(
                df=df,
                entity_name=entity_name,
                source_folder=source_folder
            )
            LoggingUtil().log_info(f"[HorizonStandardisationProcess] Anonymisation applied for {entity_name}")
            return anonymised_df
        except Exception as e:
            LoggingUtil().log_error(f"[HorizonStandardisationProcess] Anonymisation failed: {e}")
            return df

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
                spark=SparkSession.builder.getOrCreate(),
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
            spark=SparkSession.builder.getOrCreate(),
            storage_endpoint=Util.get_storage_account(),
            container_name="odw-config",
            blob_path=f"orchestration/orchestration.json",
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
                        spark=SparkSession.builder.getOrCreate(), database_name="odw_standardised_db", table_name=table_name, file_format="delta"
                    )
                    file_map[new_entry_name] = data
                except AnalysisException as e:
                    file_map[new_entry_name] = None
                # Load standardised table schema
                if "Standardised_Table_Definition" in definition:
                    standardised_table_loc = Util.get_path_to_file(f"odw-config/{definition['Standardised_Table_Definition']}")
                    standardised_table_schema = json.loads(self.spark.read.text(standardised_table_loc, wholetext=True).first().value)
                else:
                    standardised_table_schema = SchemaUtil(db_name="odw_standardised_db").get_schema_for_entity(definition["Source_Frequency_Folder"])
                file_map[f"{table_name}_standardised_table_schema"] = standardised_table_schema
            else:
                file_map[new_entry_name] = None
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
            table_name = definition.get("Standardised_Table_Name", None)
            processed_tables.append(table_name)
            schema = source_data.get(f"{table_name}_standardised_table_schema")

            if definition:
                # Recreation of ingest adhoc
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

                # Apply anonymisation if enabled
                data = self.apply_anonymisation(data, entity_name=definition["Source_Filename_Start"], source_folder="Horizon")

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
                table_name=f", ".join(processed_tables),
                insert_count=new_row_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )


### Module odw.core.etl.transformation.standardised.service_bus_standardisation_process

#COMMENTOUT from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
#COMMENTOUT from odw.core.etl.util.schema_util import SchemaUtil
#COMMENTOUT from odw.core.util.util import Util
#COMMENTOUT from odw.core.util.logging_util import LoggingUtil
#COMMENTOUT from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
#COMMENTOUT from odw.core.io.synapse_table_data_io import SynapseTableDataIO
#COMMENTOUT from odw.core.anonymisation import AnonymisationEngine, load_config
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict
import re
import os


class ServiceBusStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising the raw data from the Service Bus
    with integrated anonymisation support (DEV/TEST only)
    """
    
    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)
        self.enable_anonymisation = os.getenv("ODW_ENABLE_ANONYMISATION", "true").lower() == "true"
        self.anonymisation_engine = None
        if self.enable_anonymisation:
            self._init_anonymisation()
    
    def _init_anonymisation(self):
        """Initialize the anonymisation engine with optional config."""
        try:
            config_path = os.getenv("ODW_ANONYMISATION_CONFIG_PATH")
            config = None
            
            if config_path:
                try:
                    config = load_config(path=config_path)
                    LoggingUtil().log_info(f"Loaded anonymisation config from {config_path}")
                except Exception as e:
                    LoggingUtil().log_warning(f"Failed to load anonymisation config: {e}. Using defaults.")
            
            self.anonymisation_engine = AnonymisationEngine(config=config)
            LoggingUtil().log_info("Anonymisation engine initialized for standardisation (DEV/TEST)")
        except Exception as e:
            LoggingUtil().log_error(f"Failed to initialize anonymisation engine: {e}")
            self.enable_anonymisation = False
    
    def apply_anonymisation(self, df: DataFrame, entity_name: str) -> DataFrame:
        """
        Apply anonymisation to standardised DataFrame using Purview classifications.
        
        Args:
            df: Input DataFrame (standardised raw messages)
            entity_name: Entity name for Purview lookup
        
        Returns:
            Anonymised DataFrame
        """
        if not self.enable_anonymisation or self.anonymisation_engine is None:
            LoggingUtil().log_info("Anonymisation is disabled, skipping")
            return df
        
        try:
            LoggingUtil().log_info(
                f"Applying anonymisation to standardised data for entity '{entity_name}' (DEV/TEST)"
            )
            
            # Use the simplified Purview API for Service Bus
            anonymised_df = self.anonymisation_engine.apply_from_purview(
                df=df,
                entity_name=entity_name,
                source_folder="ServiceBus"
            )
            
            LoggingUtil().log_info("Anonymisation completed successfully")
            return anonymised_df
            
        except Exception as e:
            # Log but don't fail the pipeline if anonymisation fails
            LoggingUtil().log_error(
                f"Anonymisation failed for '{entity_name}': {str(e)}. Continuing without anonymisation."
            )
            return df
    
    @classmethod
    def get_name(cls):
        return "Service Bus Standardisation"

    @LoggingUtil.logging_to_appins
    def get_max_file_date(self, df: DataFrame) -> datetime:
        """
        Gets the maximum date from a file path field in a DataFrame.
        E.g. if the input_file field contained paths such as this:
        abfss://odw-raw@pinsstodwdevuks9h80mb.dfs.core.windows.net/ServiceBus/appeal-has/2024-12-02/appeal-has_2024-12-02T16:54:35.214679+0000.json
        It extracts the date from the string for each row and gets the maximum date.

        :param DataFrame df: The dataframe to analyse
        :return: The maximum file date
        """
        try:
            date_pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{4})"
            df = df.withColumn("file_date", F.regexp_extract(df["input_file"], date_pattern, 1).cast(TimestampType()))
            max_timestamp: datetime = df.agg(F.max("file_date")).collect()[0][0]
            return max_timestamp
        except Exception as e:
            error_message = f"Error extracting maximum file date from DataFrame: {str(e)}"
            LoggingUtil().log_error(error_message)
            raise

    @LoggingUtil.logging_to_appins
    def get_missing_files(self, df: DataFrame, source_path: str) -> list:
        """
        Gets the difference between the files in the source path and the files in the table.
        Converts the table column "filename" into a set.
        Creates a set containing all the files int he source path.
        Compares the two sets to give the missing files not yet loaded to the table.

        :param DataFrame df: The dataframe to analyse
        :param str source_path: The source directory to compare the dataframe against
        e.g. abfss://odw-raw@pinsstodwdevuks9h80mb.dfs.core.windows.net/ServiceBus/appeal-has/
        :return: A list of missing files not yet loaded to the table
        """
        try:
            files_in_path: set = set(self.get_all_files_in_directory(source_path))
            files_in_table: set = set(df.select("input_file").rdd.flatMap(lambda x: x).collect())
            return list(files_in_path - files_in_table)
        except Exception as e:
            error_message = f"Error getting missing files from source path '{source_path}': {str(e)}"
            LoggingUtil().log_error(error_message)
            raise

    @LoggingUtil.logging_to_appins
    def extract_and_filter_paths(self, files: list, filter_date: datetime):
        """
        Takes a list of file paths and filters them to return the file paths greater than the filter_date

        :param List[str] files: A list of file paths
        :param datetime filter_date: The date to filter on
        :return: A list of file paths greater than the given date
        """

        def inner_condition(file: str):
            print(type(file))
            timestamp_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}[:_]\d{2}[:_]\d{2}[.\d]*[+-]\d{4})")
            match = timestamp_pattern.search(file)
            return match and datetime.strptime(match.group(1).replace("_", ":"), "%Y-%m-%dT%H:%M:%S.%f%z") > filter_date

        return [file for file in files if inner_condition(file)]

    @LoggingUtil.logging_to_appins
    def read_raw_messages(self, filtered_paths: list[str], spark_schema: StructType = None) -> DataFrame:
        """
        Ingests data from service bus messages stored as json files in the raw layer

        :param List[str] filtered_paths: The file paths to ingest
        :return: A DataFrame of service bus messages with additional columns needed for the standardised table
        """
        # Read JSON files from filtered paths
        df = self.spark.read.json(filtered_paths, schema=spark_schema)
        LoggingUtil().log_info(f"Found {df.count()} new rows.")
        # Adding the standardised columns
        return (
            df.withColumn("expected_from", F.current_timestamp())
            .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
            .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
            .withColumn("input_file", F.input_file_name())
        )

    @LoggingUtil.logging_to_appins
    def remove_data_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Dedupes a DataFrame based on certain columns

        :param DataFrame: The dataframe to remove duplicates from
        :return: The cleaned dataframe
        """
        # removing duplicates while ignoring the ingestion dates columns
        columns_to_ignore = {"expected_to", "expected_from", "ingested_datetime"}
        df = df.dropDuplicates(subset=[c for c in df.columns if c not in columns_to_ignore])
        return df
    
    def load_data(self, **kwargs):
        entity_name: str = kwargs.get("entity_name", None)
        if not entity_name:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a entity_name to be provided, but was missing")
        use_max_date_filter = kwargs.get("use_max_date_filter", False)
        database_name = "odw_standardised_db"
        table_name = f"sb_{entity_name.replace('-', '_')}"
        source_path = Util.get_path_to_file(f"odw-raw/ServiceBus/{entity_name}")

        table_df = SynapseTableDataIO().read(
            spark=SparkSession.builder.getOrCreate(),
            database_name=database_name,
            table_name=table_name,
            file_format="delta"
        )

        max_extracted_date = self.get_max_file_date(table_df)
        missing_files = self.get_missing_files(table_df, source_path)
        filtered_paths = []
        if use_max_date_filter:
            filtered_paths = self.extract_and_filter_paths(self.get_all_files_in_directory(source_path=source_path), max_extracted_date)
        new_raw_messages = self.read_raw_messages(
            missing_files + filtered_paths,
            SchemaUtil(db_name="odw_standardised_db").get_service_bus_schema(entity_name)
        )
        return {
            f"{database_name}.{table_name}": table_df,
            "raw_messages": new_raw_messages
        }

    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a source_data dictionary to be provided, but was missing")
        entity_name: str = kwargs.get("entity_name", None)
        if not entity_name:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a entity_name to be provided, but was missing")
        database_name = "odw_standardised_db"
        table_name = f"sb_{entity_name.replace('-', '_')}"
        table_path: str = f"{database_name}.{table_name}"

        table_df = source_data.pop(table_path, None)
        if not table_df:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a source_data dataframe to be provided, but was missing")

        new_raw_messages = source_data.pop("raw_messages", None)
        if not new_raw_messages:
            # todo check
            raise ValueError()
        table_row_count = table_df.count()
        new_raw_messages = self.remove_data_duplicates(new_raw_messages)
        
        # Apply anonymisation BEFORE writing to standardised layer (DEV/TEST)
        new_raw_messages = self.apply_anonymisation(new_raw_messages, entity_name)
        
        insert_count = new_raw_messages.count()
        LoggingUtil().log_info(f"Rows to append: {insert_count}")
        expected_new_count = table_row_count + insert_count
        LoggingUtil().log_info(f"Expected new count: {expected_new_count}")
        end_exec_time = datetime.now()
        return {
            table_path: {
                "data": new_raw_messages,
                "storage_kind": "ADLSG2-Table",
                "database_name": database_name,
                "table_name": table_name,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": table_name,
                "file_format": "delta",
                "write_mode": "append",
                "write_options": [("mergeSchema", "true")],
            }
        }, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=table_name,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )


### Module odw.core.etl.etl_process_factory

#COMMENTOUT from odw.core.etl.etl_process import ETLProcess
#COMMENTOUT from odw.core.exceptions import DuplicateETLProcessNameException, ETLProcessNameNotFoundException
#COMMENTOUT from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
#COMMENTOUT from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
#COMMENTOUT from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
#COMMENTOUT from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess
from typing import Dict, List, Set, Type
import json


class ETLProcessFactory():
    ETL_PROCESSES: Set[Type[ETLProcess]] = {
        StandardisationProcess,
        ServiceBusStandardisationProcess,
        HorizonStandardisationProcess,
        ServiceBusHarmonisationProcess
    }

    @classmethod
    def _validate_etl_process_classes(cls):
        name_map: Dict[str, List[Type[ETLProcess]]] = dict()
        for etl_process_class in cls.ETL_PROCESSES:
            type_name = etl_process_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(etl_process_class)
            else:
                name_map[type_name] = [etl_process_class]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateETLProcessNameException(
                f"The following ETLProcess implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, etl_process_name: str) -> Type[ETLProcess]:
        etl_process_map = cls._validate_etl_process_classes()
        if etl_process_name not in etl_process_map:
            raise ETLProcessNameNotFoundException(
                f"No ETLProcess class could be found for ETLProcess name '{etl_process_name}'"
            )
        return etl_process_map[etl_process_name]