from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.util.util import Util
import mock


def test_get_storage():
    mock_connection_string = "url=https://mystorageaccount.dfs.core.windows.net/;token=[REDACTED]"
    with mock.patch.object(notebookutils.mssparkutils.credentials, "getFullConnectionString", return_value=mock_connection_string):
        storage_account = Util.get_storage_account()
        expected_storage_account = "mystorageaccount.dfs.core.windows.net/"
        assert storage_account == expected_storage_account
