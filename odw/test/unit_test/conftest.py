from odw.test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401


def pytest_sessionstart(session):
    # Initialise the first spark session with the below settings
    configure_session()
