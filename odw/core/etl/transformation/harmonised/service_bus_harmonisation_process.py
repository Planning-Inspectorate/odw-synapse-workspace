from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.util.udf import absolute
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
