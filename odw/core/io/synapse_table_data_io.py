from odw.core.io.synapse_data_io import SynapseDataIO
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
        :param str storage_name: The name of the storage account to write the underlying data to
        :param str container_name: The container to write the underlying data to
        :param str blob_path: The path to the blob (in the container) to write the underlying data to
        :param str file_format: The underlying file format of the table to write
        :param str write_mode: The pyspark write mode for writing the underlying data
        """
        database_name = kwargs.get("database_name", None)
        table_name = kwargs.get("table_name", None)
        storage_name = kwargs.get("storage_name", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        file_format = kwargs.get("file_format", None)
        write_mode = kwargs.get("write_mode", None)
        if not database_name:
            raise ValueError(f"SynapseTableDataIO.read requires a database_name to be provided, but was missing")
        if not table_name:
            raise ValueError(f"SynapseTableDataIO.read requires a table_name to be provided, but was missing")
        if not storage_name:
            raise ValueError(f"SynapseTableDataIO.read requires a storage_name to be provided, but was missing")
        if not container_name:
            raise ValueError(f"SynapseTableDataIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"SynapseTableDataIO.read requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError(f"SynapseTableDataIO.read requires a file_format to be provided, but was missing")
        if not write_mode:
            raise ValueError(f"SynapseDeltaDataIO.write requires a write_mode to be provided, but was missing")
        table_path = f"{database_name}.{table_name}"
        data_path = self._format_to_adls_path(storage_name, container_name, blob_path)
        data.write.format(file_format).mode(write_mode).option("path", data_path).saveAsTable(table_path)
