from odw.core.io.synapse_data_io import SynapseDataIO
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
            raise ValueError("SynapseFileDataIO.read requires a spark to be provided, but was missing")
        if not (storage_name or storage_endpoint):
            raise ValueError("SynapseFileDataIO.read expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError("SynapseFileDataIO.read expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError("SynapseFileDataIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError("SynapseFileDataIO.read requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError("SynapseFileDataIO.read requires a file_format to be provided, but was missing")
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
            raise ValueError("SynapseFileDataIO.write expected one of 'storage_name' or 'storage_endpoint' to be provided")
        if storage_name and storage_endpoint:
            raise ValueError("SynapseFileDataIO.write expected only one of 'storage_name' or 'storage_endpoint' to be provided, not both")
        if not container_name:
            raise ValueError("SynapseFileDataIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError("SynapseFileDataIO.write requires a blob_path to be provided, but was missing")
        if not file_format:
            raise ValueError("SynapseDeltaDataIO.write requires a file_format to be provided, but was missing")
        if not write_mode:
            raise ValueError("SynapseDeltaDataIO.write requires a write_mode to be provided, but was missing")
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
