from tests.util.conftest_util import ConftestUtil
import pytest


def pytest_addoption(parser):
    # input location at landing zone
    parser.addoption("--client_id", action="store")
    # input location at landing zone
    parser.addoption("--client_secret", action="store")
    # input location at landing zone
    parser.addoption("--tenant", action="store")


@pytest.fixture()
def pipeline_name(pytestconfig: pytest.Config) -> str:
    return pytestconfig.getoption("pipeline")


@pytest.fixture()
def credential_name() -> str:
    return ConftestUtil.get_credential_endpoint()


@pytest.fixture()
def synapse_endpoint() -> str:
    return ConftestUtil.get_synapse_endpoint()


@pytest.fixture()
def azure_credential(pytestconfig: pytest.Config):
    return ConftestUtil.get_azure_credential(
        pytestconfig.getoption("client_id"),
        pytestconfig.getoption("client_secret"),
        pytestconfig.getoption("tenant")
    )
