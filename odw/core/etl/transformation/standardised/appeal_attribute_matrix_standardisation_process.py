from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
import re
from types import SimpleNamespace
from typing import Any

try:
    from notebookutils import mssparkutils
except ImportError:  # pragma: no cover
    mssparkutils = SimpleNamespace(
        fs=SimpleNamespace(ls=None),
        notebook=SimpleNamespace(run=None),
    )


class AppealAttributeMatrixStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
        self.logger = LoggingUtil(self.__class__.__name__)

    def get_name(self) -> str:
        return "appeal_attribute_matrix_standardisation_process"

    def _get_latest_ingestion_date(self, entry_names: list[str]) -> str:
        date_dirs = [entry.strip("/").split("/")[-1] for entry in entry_names if re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry.strip("/").split("/")[-1])]

        if not date_dirs:
            raise FileNotFoundError("No YYYY-MM-DD folders found")

        return max(date_dirs)

    def _to_camel_case(self, colname: str) -> str:
        parts = re.split(r"\s+", colname.strip())
        return parts[0].lower() + "".join(part.capitalize() for part in parts[1:])

    def load_data(self) -> dict[str, Any]:
        storage_account = Util.get_storage_account()
        base_dir = f"abfss://odw-raw@{storage_account}AppealAttributeMatrix/"
        entries = mssparkutils.fs.ls(base_dir)
        entry_names = [entry.name for entry in entries]

        ingestion_date = self._get_latest_ingestion_date(entry_names)
        file_path = f"{base_dir}{ingestion_date}/appeal-attribute-matrix.csv"

        raw_data = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .option("ignoreLeadingWhiteSpace", True)
            .option("ignoreTrailingWhiteSpace", True)
            .csv(file_path)
        )

        return {"raw_data": raw_data}

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealAttributeMatrixStandardisationProcess.process() has not been implemented yet.")
