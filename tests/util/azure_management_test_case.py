from tests.util.conftest_util import ConftestUtil
from tests.util.test_case import TestCase


class AzureManagementTestCase(TestCase):
    AZURE_MANAGEMENT_API_ACCESS_TOKEN = ConftestUtil.get_azure_credential().get_token("https://management.azure.com").token
