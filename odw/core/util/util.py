from notebookutils import mssparkutils
import re


class Util:
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

    @classmethod
    def get_path_to_file(cls, path: str):
        path_split = path.split("/", maxsplit=1)
        if not len(path_split) == 2:
            raise ValueError(f"Path should have the format 'container_name/path' but was '{path}'")
        container_name = path_split[0]
        blob_path = path_split[1]
        storage_account = cls.get_storage_account()
        return f"abfss://{container_name}@{storage_account}{blob_path}"
