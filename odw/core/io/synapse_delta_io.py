from odw.core.io.synapse_data_io import SynapseDataIO
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
