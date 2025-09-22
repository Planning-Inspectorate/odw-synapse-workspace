from tests.util.notebook_run_test_case import NotebookRunTestCase
import json


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
        exit_value = notebook_run_result["result"]["exitValue"].replace('"', '\"').replace("'", "\"")
        if exit_value:
            test_output = json.loads(exit_value)
            test_output_json = json.dumps(test_output, indent=4)
            assert not test_output, f"The following tests failed in the test_smoke_py_connectivity notebook : {test_output_json}"
