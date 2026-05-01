from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


_REFERENCE_COLUMNS = [
    "ID",
    "value",
]


class CheckmarkReadingTypeReferenceHarmonisationProcess(HarmonisationProcess):
    """
    Harmonisation process for Checkmark reading_type_reference reference data.

    Truncate-load semantics: reads all rows from the standardised reference
    table, adds IngestionDate (date) and an MD5-based RowID, and overwrites
    the harmonised table.
    """

    SOURCE_TABLE = "odw_standardised_db.readingtypereference"
    OUTPUT_TABLE = "odw_harmonised_db.reading_type_reference"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "checkmark-reading-type-reference-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading source data from {self.SOURCE_TABLE}")
        source_data = self.spark.sql(f"SELECT * FROM {self.SOURCE_TABLE}")
        return {
            "source_data": source_data,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        self.spark.sql("SET spark.sql.legacy.timeParserPolicy = LEGACY")

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("source_data", source_data)

        df = df.select(*_REFERENCE_COLUMNS)
        df = df.withColumn("IngestionDate", F.current_date())

        hash_inputs = [
            F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
            for c in _REFERENCE_COLUMNS
        ]
        df = df.withColumn("RowID", F.md5(F.concat_ws("|", *hash_inputs)))

        null_rowid_count = df.filter(F.col("RowID").isNull()).count()
        if null_rowid_count > 0:
            LoggingUtil().log_error(
                f"Data quality issue: {null_rowid_count} rows have NULL RowID values"
            )
        else:
            LoggingUtil().log_info("Data quality check passed: No NULL RowID values found")

        insert_count = df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "reading_type_reference",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "reading_type_reference",
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
            ),
        )
