from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from notebookutils import mssparkutils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from typing import Dict, Tuple

_ENTRAID_FIELDS = ["id", "employeeId", "givenName", "surname", "userPrincipalName"]


class EntraIdStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising raw EntraID user data from ADLS into the
    odw_standardised_db.entraid Delta table.

    Reads the latest (or a specified) date-stamped snapshot from odw-raw/entraid/,
    explodes the nested 'value' array, and writes the flattened records with an
    expected_from timestamp to the standardised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "EntraID Standardisation",
        "date_folder": "2024-01-15",  # optional — defaults to latest folder
        "debug": False
    }
    ```
    """

    RAW_FOLDER = "entraid"
    OUTPUT_TABLE = "odw_standardised_db.entraid"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)
        self._folder_name: str = ""

    @classmethod
    def get_name(cls) -> str:
        return "EntraID Standardisation"

    def _get_latest_folder(self, raw_path: str) -> str:
        folders = sorted(
            [f.name.rstrip("/") for f in mssparkutils.fs.ls(raw_path) if f.isDir],
            reverse=True,
        )
        if not folders:
            raise FileNotFoundError(f"No date folders found under {raw_path}")
        return folders[0]

    # ------------------------------------------------------------------
    # load_data – resolves the folder and reads the raw JSON file
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Locate and read the raw EntraID JSON snapshot from ADLS.

        The folder is resolved from the optional 'date_folder' kwarg (YYYY-MM-DD).
        If omitted, the most-recently-named folder under odw-raw/entraid/ is used.
        The folder name is stored on the instance so process() can derive expected_from.
        """
        date_folder: str = kwargs.get("date_folder", "")
        storage_endpoint = Util.get_storage_account()
        raw_path = Util.get_path_to_file(f"odw-raw/{self.RAW_FOLDER}")

        if date_folder:
            self._folder_name = date_folder
        else:
            LoggingUtil().log_info(f"No date_folder supplied — discovering latest folder under {raw_path}")
            self._folder_name = self._get_latest_folder(raw_path)

        blob_path = f"{self.RAW_FOLDER}/{self._folder_name}/entraid.json"
        LoggingUtil().log_info(f"Reading EntraID raw file: odw-raw/{blob_path}")

        raw_df = SynapseFileDataIO().read(
            spark=self.spark,
            storage_endpoint=storage_endpoint,
            container_name="odw-raw",
            blob_path=blob_path,
            file_format="json",
            read_options={"multiline": "true"},
        )

        return {"raw_entraid": raw_df}

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Explode the raw JSON 'value' array, extract the required fields, and
        append an expected_from timestamp derived from the raw folder date.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        raw_df: DataFrame = self.load_parameter("raw_entraid", source_data)

        LoggingUtil().log_info(f"Exploding EntraID records from folder {self._folder_name}")
        std_df = (
            raw_df
            .select(F.explode(F.col("value")).alias("record"))
            .select(*[F.col("record")[c].alias(c) for c in _ENTRAID_FIELDS])
        )

        # expected_from mirrors the ingest_adhoc convention: day prior to the folder date
        folder_date = datetime.strptime(self._folder_name, "%Y-%m-%d").date()
        expected_from = datetime.combine(folder_date - timedelta(days=1), datetime.min.time())
        std_df = std_df.withColumn("expected_from", F.to_timestamp(F.lit(str(expected_from))))

        insert_count = std_df.count()
        LoggingUtil().log_info(f"Standardised {insert_count} EntraID records into {self.OUTPUT_TABLE}")

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": std_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_standardised_db",
                "table_name": "entraid",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": "entraid",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
