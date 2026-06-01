from odw.core.etl.etl_result import ETLResult
from odw.core.io.synapse_delta_io import SynapseDeltaIO
from odw.core.util.util import Util
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from datetime import datetime
from typing import Dict, Any
import json


class MetadataManager:
    """
    Class to handle the management of the execution_history metadata table
    """

    METADATA_DB = "odw_meta_db"
    METADATA_CONTAINER = "odw-meta-db"
    METADATA_TABLE = "odw_execution_history"
    METADATA_SCHEMA = StructType(
        [
            StructField("run_id", StringType(), False, metadata={"comment": "The id of the master pipeline that triggered the ETL process"}),
            StructField(
                "entity_name",
                StringType(),
                False,
                metadata={"comment": "The name of the entity, which aligns with orchestration_transform_dependencies.yaml"},
            ),
            StructField(
                "stage_name",
                StringType(),
                False,
                metadata={
                    "comment": "The stage name of the entity that is being executed, which aligns with orchestration_transform_dependencies.yaml"
                },
            ),
            StructField("execution_parameters", StringType(), True, metadata={"comment": "The parameters passed to the ETL Process"}),
            StructField("execution_start_time", TimestampType(), False, metadata={"comment": "When the entry was added to the metadata table"}),
            StructField(
                "execution_finish_time",
                TimestampType(),
                True,
                metadata={"comment": "When the entry was updated with either success or failure details"},
            ),
            StructField(
                "successful",
                BooleanType(),
                True,
                metadata={"comment": "If the complete pipeline was finished or not. If null, then the process has not finished"},
            ),
            StructField(
                "result_text",
                StringType(),
                True,
                metadata={
                    "comment": "String form of the result. This will either be an ETLResult (in json format), or the error message depending on the value of the 'successful' column"
                },
            ),
            StructField("_update_key_col", StringType(), False),  # Not included in the table, used for handling the delta merge logic
        ]
    )

    def __init__(
        self, spark: SparkSession, run_id: str, entity_name: str = None, stage_name: str = None, execution_parameters: Dict[str, Any] = None
    ):
        """
        :param spark SparkSession: The underlying pyspark session
        :param run_id str: The run id of the exection history entries to manage. MUST be supplied to write metadata
        :param entity_name str: The entity name of the execution history entries to manage. MUST be supplied to write metadata
        :param stage_name str: The stage name of the execution history entry to manage. MUST be supplied to write metadata
        :param execution_parameters dict: The kwargs supplied to the calling ETLProcess. MUST be supplied to write metadata
        """
        if not run_id:
            raise RuntimeError("Requires a run_id to be provided")
        self._created_entry = False
        self._updated_entry = False
        self._spark = spark
        self._entry = {
            "run_id": run_id,
            "entity_name": entity_name,
            "stage_name": stage_name,
            "execution_parameters": json.dumps(execution_parameters, default=str),
            "execution_start_time": datetime.now(),
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
            "_update_key_col": "create",
        }

    def _write(self, data: DataFrame):
        SynapseDeltaIO().write(
            data,
            spark=self._spark,
            storage_endpoint=Util.get_storage_account(),
            database_name=self.METADATA_DB,
            table_name=self.METADATA_TABLE,
            container_name=self.METADATA_CONTAINER,
            blob_path=self.METADATA_TABLE,
            merge_keys=["run_id", "entity_name", "stage_name"],
            update_key_col="_update_key_col",
            columns_to_update=["execution_finish_time", "successful", "result_text"],
            partition_by_cols=["run_id"],
        )

    def create(self):
        """
        Add an entry for this particular execution instance to the execution history table with the given execution parameters
        """
        run_id = self._entry.get("run_id", None)
        entity_name = self._entry.get("entity_name", None)
        stage_name = self._entry.get("stage_name", None)
        execution_parameters = self._entry.get("execution_parameters", None)
        if not (run_id and entity_name and stage_name and execution_parameters):
            raise ValueError(
                "Must supply run_id, entity_name, stage_name, and execution_parameters when creating an entry, but some of these were missing"
            )
        if self._created_entry:
            raise RuntimeError(
                f"Metadata entry has already been created for the given run_id, entity_name, stage_name ('{run_id}', '{entity_name}', '{stage_name}')"
            )
        if not self._created_entry:
            self._created_entry = True
            data_to_create = self._spark.createDataFrame([self._entry], schema=self.METADATA_SCHEMA)
            self._write(data_to_create)

    def update(self, etl_result: ETLResult):
        """
        Update the entry for this particular execution instance with the given etl result information
        """
        run_id = self._entry.get("run_id", None)
        entity_name = self._entry.get("entity_name", None)
        stage_name = self._entry.get("stage_name", None)
        if not (run_id and entity_name and stage_name):
            raise ValueError("Must supply run_id, entity_name, and stage_name when updating an entry, but some of these were missing")
        if self._updated_entry or not self._created_entry:
            raise RuntimeError(f"Cannot update the metadata entry before the entry has been created")
        if self._created_entry and not self._updated_entry:
            self._updated_entry = True
            result_metadata: ETLResult.ETLResultMetadata = etl_result.metadata
            end_execution_time = result_metadata.end_execution_time
            successful = etl_result.outcome == "Succeeded"
            self._entry |= {
                "execution_finish_time": end_execution_time,
                "successful": successful,
                "result_text": etl_result.model_dump_json(indent=4),
                "_update_key_col": "update",
            }
            data_to_update = self._spark.createDataFrame([self._entry], schema=self.METADATA_SCHEMA)
            self._write(data_to_update)

    def get_for_run_id(self):
        """
        Return all entries for the run_id
        """
        run_id = self._entry.get("run_id", None)
        return self._spark.sql(f"select * from {self.METADATA_DB}.{self.METADATA_TABLE} where run_id = '{run_id}'")

    def get_for_entity(self):
        """
        Return all entries for the run_id, entity_name pair
        """
        run_id = self._entry.get("run_id", None)
        entity_name = self._entry.get("entity_name", None)
        if not (run_id and entity_name):
            raise RuntimeError("Requires run_id and entity_name to be provided")
        return self._spark.sql(f"select * from {self.METADATA_DB}.{self.METADATA_TABLE} where run_id = '{run_id}' and entity_name = '{entity_name}'")

    def get_for_entity_stage(self):
        """
        Return all entries for a specific run_id, entity_name, stage_name group
        """
        run_id = self._entry.get("run_id", None)
        entity_name = self._entry.get("entity_name", None)
        stage_name = self._entry.get("stage_name", None)
        if not (run_id and entity_name and stage_name):
            raise RuntimeError("Requires run_id, entity_name and stage_name to be provided")
        return self._spark.sql(
            f"select * from {self.METADATA_DB}.{self.METADATA_TABLE} where run_id = '{run_id}' and entity_name = '{entity_name}' and stage_name = '{stage_name}'"
        )
