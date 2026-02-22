class TestCase:
    """
    Represents a test case with setup and teardown methods. This is to support using pytestx-dist, whic
    does not respect pytest's session-level fixtures

    If you need a setup or teardown to be called at the session-level withpytest-xdist, then
    create your test using the test case as the parent class, and implement one of the below methods
    """

    def session_setup(self):
        """
        Called once before testing begins
        """
        pass

    def session_teardown(self):
        """
        Called once after all tests have finished
        """
        pass

    def setup(self):
        """
        Called before each test
        """
        pass

    def teardown(self):
        """
        Called after each test
        """
        pass
