from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from notebookutils import mssparkutils
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datetime import datetime
from typing import Dict
import re
from odw.core.util.logging_util import LoggingUtil


class AppealAttributeMatrixStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "appeal_attribute_matrix"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_attribute_matrix_standardisation_process"

    def _get_latest_ingestion_date(self, entry_names):
        date_dirs = [e.strip("/").split("/")[-1] for e in entry_names if re.fullmatch(r"\d{4}-\d{2}-\d{2}", e.strip("/").split("/")[-1])]

        if not date_dirs:
            raise FileNotFoundError(f"No YYYY-MM-DD folders found under {entry_names}")
        return max(date_dirs)

    def _to_camel_case(self, colname: str) -> str:
        parts = re.split(r"\s+", colname.strip())
        return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        storage_account = Util.get_storage_account()

        base_dir = f"abfss://odw-raw@{storage_account}AppealAttributeMatrix/"
        entries = mssparkutils.fs.ls(base_dir)

        entry_names = [e.name for e in entries]

        ingestion_date = self._get_latest_ingestion_date(entry_names)

        file_path = f"{base_dir}{ingestion_date}/appeal-attribute-matrix.csv"

        raw_df = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .option("ignoreLeadingWhiteSpace", True)
            .option("ignoreTrailingWhiteSpace", True)
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(file_path)
        )

        return {"raw_data": raw_df}

    def process(self, **kwargs):
        start_exec_time = datetime.now()

        source_data = self.load_parameter("source_data", kwargs)
        raw_df: DataFrame = self.load_parameter("raw_data", source_data)

        valid_columns = [
            field.name for field in raw_df.schema.fields if field.name and field.name.strip() != "" and not field.name.strip().startswith("_c")
        ]

        df = raw_df.select(*valid_columns)

        df = df.filter(~(col("attribute").isNull() | col("attribute").rlike(r'^\s*["\']?\s*$')))

        df = df.select(*[col(c).alias(self._to_camel_case(c)) for c in df.columns])

        if "s78" in df.columns:
            df = df.withColumn("s78", col("s78").cast("string"))

        try:
            storage_endpoint = Util.get_storage_account()
        except Exception:
            storage_endpoint = "test_storage"

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_standardised_db",
                "table_name": "appeal_attribute_matrix",
                "storage_endpoint": storage_endpoint,
                "container_name": "odw-standardised",
                "blob_path": "appeal_attribute_matrix",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }

        end_exec_time = datetime.now()

        result = ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=df.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.get_name(),
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
        return data_to_write, result
