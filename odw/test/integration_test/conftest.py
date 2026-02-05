from odw.test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401
from odw.test.util.session_util import PytestSparkSessionUtil


DATABASE_NAMES = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]


def pytest_sessionstart(session):
    # Initialise the first spark session with the below settings
    PytestSparkSessionUtil()
    configure_session()
