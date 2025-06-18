from tests.util.notebook_run_test_case import NotebookRunTestCase
from tests.util.synapse_util import SynapseUtil
from azure.core.exceptions import ResourceNotFoundError
from tests.util.config import TEST_CONFIG
import pandas as pd
from io import BytesIO


class TestIntegrationPyDeleteTable(NotebookRunTestCase):
    target_container = "odw-standardised"
    target_blob = "test/test_py_delete_table.parquet"
    def module_setup(self):
        try:
            SynapseUtil.delete_blob(TEST_CONFIG["DATA_LAKE_STORAGE"], self.target_container, self.target_blob)
        except ResourceNotFoundError:
            pass
        df = pd.DataFrame(
            {
                "colA": [1, 2, 3],
                "colB": [4, 5, 6],
                "colC": [7, 8, 9]
            },
            dtype=int
        )
        tmp = BytesIO()
        df.to_parquet(tmp)
        tmp.seek(0)
        SynapseUtil.upload_blob(tmp, TEST_CONFIG["DATA_LAKE_STORAGE"], self.target_container, self.target_blob)

    def module_teardown(self):
        try:
            SynapseUtil.delete_blob(TEST_CONFIG["DATA_LAKE_STORAGE"], "odw-standardised", "test/test_py_delete_table.parquet")
        except ResourceNotFoundError:
            pass

    def test_py_delete_table(self):
        # Unfortunately we cannot create tables in the lake database from outside of Synapse, so the rest of the test is done in Synapse
        self.run_notebook(
            "test_py_delete_table",
            {
                "TARGET_CONTAINER": {
                    "type": "String",
                    "value": self.target_container
                },
                "TARGET_BLOB": {
                    "type": "String",
                    "value": self.target_blob
                }
            },
            max_wait_time_minutes = 30
        )
