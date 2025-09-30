from tests.util.notebook_run_test_case import NotebookRunTestCase
import ast


class TestSmokeSynapseConnectivity(NotebookRunTestCase):
    """
        This test verifies that external connections from synapse are working properly

        The following connections are tested:
        - data lake
        - failover data lake
        - key vault
    """
    def test_synapse_connectivity(self):
        notebook_name = "test_smoke_py_connectivity"
        notebook_parameters = dict()

        notebook_run_result = self.run_notebook(notebook_name, notebook_parameters)
        exit_value = notebook_run_result["result"]["exitValue"]
        if exit_value:
            exit_value_cleaned = ast.literal_eval(exit_value)
            assertion_message = "\n\n".join(
                f"Test {test} failed with the below error traceback\n {error}" for test, error in exit_value_cleaned.items()
            )
            assert not exit_value_cleaned, f"The following tests failed in the test_smoke_py_connectivity notebook\n{assertion_message}"
