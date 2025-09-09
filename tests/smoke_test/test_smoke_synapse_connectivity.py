from tests.util.notebook_run_test_case import NotebookRunTestCase
import json
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
        
        # Parse the exit value - it might be JSON or a Python literal
        try:
            failing_tests = json.loads(exit_value)
        except json.JSONDecodeError:
            # If JSON parsing fails, try parsing as Python literal (e.g., ['test_name'])
            failing_tests = ast.literal_eval(exit_value)
            
        assert not failing_tests, f"The following tests failed in the test_smoke_py_connectivity notebook : {json.dumps(failing_tests, indent=4)}"
