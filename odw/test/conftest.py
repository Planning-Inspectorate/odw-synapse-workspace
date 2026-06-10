"""
Root conftest — keeps the unit-test suite runnable locally without valid Azure CLI credentials.

Problem: odw/test/util/conftest_util.py::import_all_testing_modules() eagerly imports every
file under odw/test/, including odw/test/integration_test/test_logging_util.py which calls
AzureCliCredential().get_token(...) at module scope.  When the local Azure CLI token is
expired this raises ClientAuthenticationError and crashes the whole session.

Fix: wrap pytest_sessionstart so that AzureCliCredential is patched to a no-op mock for the
duration of the eager import.  Integration tests that run later and actually need real
credentials are unaffected because they are skipped by the -m "not integration" marker filter
used for local unit-test runs.
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.hookimpl(hookwrapper=True)
def pytest_sessionstart(session):
    _mock_token = MagicMock()
    _mock_token.token = "mock-token-for-local-unit-test-run"
    with patch("azure.identity.AzureCliCredential") as _mock_cred:
        _mock_cred.return_value.get_token.return_value = _mock_token
        yield
