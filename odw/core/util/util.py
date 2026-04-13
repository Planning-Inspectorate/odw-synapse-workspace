from notebookutils import mssparkutils
from notebookutils import visualization
from pyspark.sql import DataFrame, SparkSession
import re


class Util:
    """
    Class that defines utility functions
    """

    @classmethod
    def get_environment(cls) -> str:
        """
        Return the current environment (DEV, TEST, or PROD).

        Resolution order:
        1. Spark configuration 'spark.executorEnv.environment' (set by the attached SparkConfiguration artifact).
        2. Synapse workspace name — looks for 'dev', 'test', 'preprod' or 'prod' as a substring.
        3. Defaults to PROD as a fail-safe.
        """
        try:
            spark = SparkSession.builder.getOrCreate()
            env = spark.sparkContext.getConf().get("spark.executorEnv.environment", "")
            if env:
                return env.upper()
        except Exception:
            pass
        # Fallback: infer from the Synapse workspace name
        try:
            workspace_name = mssparkutils.env.getWorkspaceName().lower()
            if "dev" in workspace_name:
                return "DEV"
            if "preprod" in workspace_name:
                return "PREPROD"
            if "test" in workspace_name:
                return "TEST"
            if "prod" in workspace_name:
                return "PROD"
        except Exception:
            pass
        return "PROD"  # Fail-safe default

    @classmethod
    def is_non_production_environment(cls) -> bool:
        """
        Return True if the current environment is DEV only.
        """
        return cls.get_environment() == "DEV"

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

    @classmethod
    def display_dataframe(cls, dataframe: DataFrame):
        """
        Show the contents of the given dataframe. This is a wrapper of the display() function in Synapse
        """
        visualization.display(dataframe)
