from tests.util.config import TEST_CONFIG
from tests.util.notebook_run_test_case import NotebookRunTestCase


class TestUnitPyUtilsGetStorageAccount(NotebookRunTestCase):
    def test_py_utils_get_storage_account(self):
        return_value = self.run_notebook("py_utils_get_storage_account", dict())
        expected_exit_value = f"{TEST_CONFIG['DATA_LAKE_STORAGE']}.dfs.core.windows.net/"
        notebook_result = return_value.get("result", None)
        assert notebook_result, f"Expected a 'result' property to be returned by the notebook run, but the run only contained {list(return_value.keys())}"
        exit_value = notebook_result.get("exitValue", None)
        assert exit_value == expected_exit_value, f"Notebook 'exitValue` had an unexpected value. Expected '{expected_exit_value}' but was '{exit_value}'"
