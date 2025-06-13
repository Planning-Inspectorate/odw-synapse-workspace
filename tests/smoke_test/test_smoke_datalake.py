from tests.util.conftest_util import ConftestUtil
from tests.util.config import TEST_CONFIG
from tests.util.test_case import TestCase
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError


class TestSmokeDatalake(TestCase):
    def check_storage_account(self, account_url: str):
        blob_service_client = BlobServiceClient(account_url, credential=ConftestUtil.get_azure_credential())
        exception = None
        try:
            blob_service_client.get_service_properties()
        except HttpResponseError as e:
            exception = e
        assert not exception, (
            f"Could not get the details of the storage account {account_url}. "
            f"The following error was raised by the BlobServiceClient: {exception}"
        )

    def test_datalake_exists(self):
        account_url = f"https://{TEST_CONFIG['DATA_LAKE_STORAGE']}.blob.core.windows.net"
        self.check_storage_account(account_url)
