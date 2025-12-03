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
