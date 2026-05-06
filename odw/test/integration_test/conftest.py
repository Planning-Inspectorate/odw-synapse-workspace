from odw.test.util.mock import import_mock_notebook_utils  # noqa: F401 - must be imported before configure_session to mock notebookutils
from odw.test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401


DATABASE_NAMES = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]


def pytest_sessionstart(session):
    # Initialise the first spark session with the below settings
    configure_session()
