from tests.util.config import TEST_CONFIG
from tests.util.notebook_run_test_case import NotebookRunTestCase


class TestPyUtilsGetKeyvault(NotebookRunTestCase):
    def test_py_utils_get_keyvault(self):
        test_secret_name = "test_secret_name"
        return_value = self.run_notebook(
            "py_utils_get_keyvault",
            {
                "secret_name": {
                    "type": "String",
                    "value": test_secret_name
                    }
            }
        )
        expected_exit_value = f"https://pinskvsynwodw{TEST_CONFIG['ENV'].lower()}uks.vault.azure.net/secrets/{test_secret_name}?api-version=7.4"
        notebook_result = return_value.get("result", None)
        assert notebook_result, f"Expected a 'result' property to be returned by the notebook run, but the run only contained {list(return_value.keys())}"
        exit_value = notebook_result.get("exitValue", None)
        assert exit_value == expected_exit_value, f"Notebook 'exitValue` had an unexpected value. Expected '{expected_exit_value}' but was '{exit_value}'"
