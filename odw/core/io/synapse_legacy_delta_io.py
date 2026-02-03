from odw.core.io.synapse_delta_io import SynapseDeltaIO
from odw.core.util.table_util import TableUtil
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
        temp_table_name = f"{table_name}_tmp"
        TableUtil().delete_table_contents(database_name, temp_table_name)
        writer = data.write.format("delta").mode("overwrite")
        writer.saveAsTable(f"{database_name}.{temp_table_name}")
        TableUtil().delete_table_contents(database_name, table_name)
        spark.sql(f"ALTER TABLE {database_name}.{temp_table_name} RENAME TO {database_name}.{table_name}")
