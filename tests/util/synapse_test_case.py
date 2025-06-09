from tests.util.conftest_util import ConftestUtil
from tests.util.test_case import TestCase


class SynapseTestCase(TestCase):
    SYNAPSE_ACCESS_TOKEN = ConftestUtil.get_azure_credential().get_token(ConftestUtil.get_credential_endpoint()).token
    SYNAPSE_ENDPOINT = ConftestUtil.get_synapse_endpoint()
