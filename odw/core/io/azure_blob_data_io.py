from odw.core.io.data_io import DataIO
from odw.core.io.parser.data_file_parser_factory import DataFileParserFactory
from io import BytesIO
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame


class AzureBlobDataIO(DataIO):
    @classmethod
    def get_name(cls) -> str:
        return "AzureBlob"

    """
    Manages data io to/from Azure Data Lake Storage Gen 2

    # Example usage
    ## Reading
    ```
    data_frame = AzureBlobDataIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark,
        parser_type="parquet"
    )
    ```
    ## Writing

    ```
    AzureBlobDataIO().write(
        data_frame,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark,
        parser_type="parquet"
    )
    ```
    """
    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a byte stream

        :param str storage_name: The name of the storage account to read from
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        :param SparkSession spark: The spark session
        :param str parser_type: The DataFileParser type to use
        
        :return DataFrame: The data
        """
        spark = kwargs.get("spark", None)
        storage_name = kwargs.get("storage_name", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        parser_type = kwargs.get("parser_type", None)
        if not spark:
            raise ValueError(f"AzureBlobDataIO.read requires a spark to be provided, but was missing")
        if not storage_name:
            raise ValueError(f"AzureBlobDataIO.read requires a storage_name to be provided, but was missing")
        if not container_name:
            raise ValueError(f"AzureBlobDataIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"AzureBlobDataIO.read requires a blob_path to be provided, but was missing")
        if not parser_type:
            raise ValueError(f"AzureBlobDataIO.read requires a parser_type to be provided, but was missing")
        parser_class = DataFileParserFactory.get(parser_type)(spark)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            AzureCliCredential()
        )
        storage_endpoint = f"https://{storage_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(storage_endpoint, credential=credential)
        container_client = blob_service_client.get_container_client(container_name)
        byte_stream = BytesIO()
        blob_data = container_client.download_blob(blob_path)
        blob_data.readinto(byte_stream)
        return parser_class.from_bytes(byte_stream)
    
    def write(self, data: DataFrame, **kwargs):
        """
        Write the data to the given storage location
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param SparkSession spark: The spark session
        :param str parser_type: The DataFileParser type to use
        """
        spark = kwargs.get("spark", None)
        storage_name = kwargs.get("storage_name", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        parser_type = kwargs.get("parser_type", None)
        if not spark:
            raise ValueError(f"AzureBlobDataIO.write requires a spark to be provided, but was missing")
        if not storage_name:
            raise ValueError(f"AzureBlobDataIO.write requires a storage_name to be provided, but was missing")
        if not container_name:
            raise ValueError(f"AzureBlobDataIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"AzureBlobDataIO.write requires a blob_path to be provided, but was missing")
        if not parser_type:
            raise ValueError(f"AzureBlobDataIO.write requires a parser_type to be provided, but was missing")
        parser_class = DataFileParserFactory.get(parser_type)(spark)
        data_bytes = parser_class.to_bytes(data)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            AzureCliCredential()
        )
        storage_endpoint = f"https://{storage_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(storage_endpoint, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(data_bytes, blob_type="BlockBlob")
