from odw.core.io.data_io import DataIO
from io import BytesIO
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient


class ADLSDataIO(DataIO):
    """
    Manages data io to/from Azure Data Lake Storage Gen 2

    # Example usage
    ## Reading
    ```
    data_bytes = ADLSDataIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat"
    )
    ```
    ## Writing

    ```
    ADLSDataIO().write(
        data_bytes,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat"
    )
    ```
    """
    def read(self, **kwargs) -> BytesIO:
        """
        Read from the given storage location, and return the data as a byte stream

        :param str storage_name: The name of the storage account to read from
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        
        :return BytesIO: A data as a byte stream
        """
        storage_name = kwargs.get("storage_name", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        if not storage_name:
            raise ValueError(f"ADLSDataIO.read requires a storage_name to be provided, but was missing")
        if not container_name:
            raise ValueError(f"ADLSDataIO.read requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"ADLSDataIO.read requires a blob_path to be provided, but was missing")
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
        return byte_stream
    
    def write(self, data_bytes: BytesIO, **kwargs):
        """
        Write the data bytes stream to the given storage location
        
        :param BytesIO data_bytes: The data to write
        :param str storage_name: The name of the storage account to write to
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        """
        storage_name = kwargs.get("storage_name", None)
        container_name = kwargs.get("container_name", None)
        blob_path = kwargs.get("blob_path", None)
        if not storage_name:
            raise ValueError(f"ADLSDataIO.write requires a storage_name to be provided, but was missing")
        if not container_name:
            raise ValueError(f"ADLSDataIO.write requires a container_name to be provided, but was missing")
        if not blob_path:
            raise ValueError(f"ADLSDataIO.write requires a blob_path to be provided, but was missing")
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            AzureCliCredential()
        )
        storage_endpoint = f"https://{storage_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(storage_endpoint, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(data_bytes, blob_type="BlockBlob")
