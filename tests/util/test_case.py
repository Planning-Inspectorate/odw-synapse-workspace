import pytest


class TestCase():
    """
        Generic test case class
    """
    def module_setup(self):
        """
            Initialise any common dependencies for the whole test case before testing begins
        """
        pass

    def module_teardown(self):
        """
            Remove any created components during testing after testing has completed
        """
        pass

    @pytest.fixture(scope="module", autouse=True)
    def _run_module_setup(self):
        """
            Call the module setup function when a test case starts
        """
        self.module_setup()

    @pytest.fixture(scope="module", autouse=True)
    def _run_module_teardown(self):
        """
            Call the module teardown function when a test case ends
        """
        yield
        self.module_teardown()
