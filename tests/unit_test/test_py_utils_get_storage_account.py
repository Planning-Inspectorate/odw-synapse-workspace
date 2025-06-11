from tests.util.config import TEST_CONFIG
from tests.util.notebook_run_test_case import NotebookRunTestCase


class TestPyUtilsGetStorageAccount(NotebookRunTestCase):
    def test_py_utils_get_storage_account(self):
        return_value = self.run_notebook("py_utils_get_storage_account", dict())
        expected_exit_value = f"{TEST_CONFIG['DATA_LAKE_STORAGE']}.dfs.core.windows.net/"
        exit_value = return_value["result"].get("exitValue", None)
        assert exit_value == expected_exit_value
