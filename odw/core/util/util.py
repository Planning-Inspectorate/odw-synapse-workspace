from notebookutils import mssparkutils
import re


class Util():
    """
        Class that defines utility functions
    """
    @classmethod
    def get_storage_account(cls) -> str:
        """
            Return the storage account of the Synapse workspace in the format `{storage_name}.dfs.core.windows.net/`
        """
        connection_string = mssparkutils.credentials.getFullConnectionString("ls_storage")
        return re.search("url=https://(.+?);", connection_string).group(1)
        
