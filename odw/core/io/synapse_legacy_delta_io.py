from odw.core.io.synapse_delta_io import SynapseDeltaIO
from odw.core.util.table_util import TableUtil
from pyspark.sql import DataFrame, SparkSession


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
        """
        spark: SparkSession = kwargs.get("spark", None)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        storage_name = kwargs.get("storage_name", None)
        storage_endpoint = kwargs.get("storage_endpoint", None)
        container_name = kwargs.get("container_name", None)
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        write_options = kwargs.get("write_options", dict())
        partition_cols = kwargs.get("partition_cols", [])
        if not spark:
            raise ValueError("SynapseLegacyDeltaIO.write requires spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError("SynapseLegacyDeltaIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError("SynapseLegacyDeltaIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError("SynapseLegacyDeltaIO.write requires a container_name to be provided, but was missing")
        if bool(database_name) ^ bool(table_name):
            raise ValueError("SynapseLegacyDeltaIO.write requires both database_name and table_name to be provided, or neither")
        if not isinstance(write_options, dict):
            raise ValueError(f"SynapseLegacyDeltaIO.write requires the write_options to be a dictionary of strings, but was a {type(write_options)}")
        if not isinstance(partition_cols, list):
            raise ValueError(f"SynapseLegacyDeltaIO.write requires the partition_cols to be a list of strings, but was a {type(partition_cols)}")
        temp_table_name = f"{table_name}_tmp"
        TableUtil().delete_table_contents(spark, database_name, temp_table_name)
        writer = data.write.format("delta").mode("overwrite")
        for option_name, option_value in write_options.items():
            writer.option(option_name, option_value)
        if partition_cols:
            writer.partitionBy(partition_cols)
        writer.saveAsTable(f"{database_name}.{temp_table_name}")
        TableUtil().delete_table_contents(spark, database_name, table_name)
        spark.sql(f"ALTER TABLE {database_name}.{temp_table_name} RENAME TO {database_name}.{table_name}")
