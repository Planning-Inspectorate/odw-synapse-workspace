from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.errors.exceptions.captured import AnalysisException
from typing import Any, Dict, Tuple
from datetime import datetime


class PinsInspectorCuratedProcess(CurationProcess):
    """
    ETL process for curating the PINS Inspector dataset into odw_curated_db.

    Reads active inspectors from the harmonised layer and upserts them into
    the curated Delta table using hash-based change detection.

    # Example usage via py_etl_executor

    ```
    input_arguments = {
        "etl_process_name": "PINS Inspector Curation Process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.pins_inspector"
    OUTPUT_TABLE = "odw_curated_db.pins_inspector"
    _KEY_COL = "entraId"
    _UPDATE_KEY_COL = "row_state_metadata"
    _OUTPUT_COLUMNS = [
        "entraId",
        "sapId",
        "firstName",
        "lastName",
        "email",
        "grade",
        "fte",
        "unit",
        "service",
        "group",
        "inspectorManager",
        "title",
        "address",
        "specialisms",
        "validFrom",
    ]

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "PINS Inspector Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading active inspectors from {self.HARMONISED_TABLE}")
        harmonised = self.spark.sql(f"""
            SELECT DISTINCT
                {", ".join(self._OUTPUT_COLUMNS)}
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        LoggingUtil().log_info(f"Loading existing curated data from {self.OUTPUT_TABLE}")
        try:
            curated = self.spark.sql(f"SELECT * FROM {self.OUTPUT_TABLE}")
        except AnalysisException:
            curated = self.spark.createDataFrame([], harmonised.schema)

        return {
            "harmonised_inspector": harmonised,
            "curated_inspector": curated,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        new_df: DataFrame = self.load_parameter("harmonised_inspector", source_data)
        existing_df: DataFrame = self.load_parameter("curated_inspector", source_data)

        key_col = self._KEY_COL
        new_df = new_df.filter(F.col(key_col).isNotNull())

        non_key_cols = [c for c in new_df.columns if c != key_col]
        hash_expr = F.sha2(F.to_json(F.struct(*[F.col(c) for c in non_key_cols])), 256)

        new_with_hash = new_df.withColumn("_new_hash", hash_expr)
        existing_with_hash = existing_df.select(F.col(key_col), hash_expr.alias("_existing_hash"))

        labelled = (
            new_with_hash.join(existing_with_hash, key_col, "left")
            .filter(F.col("_existing_hash").isNull() | (F.col("_new_hash") != F.col("_existing_hash")))
            .withColumn(
                self._UPDATE_KEY_COL,
                F.when(F.col("_existing_hash").isNull(), F.lit("create")).otherwise(F.lit("update")),
            )
            .drop("_new_hash", "_existing_hash")
        )

        insert_count = labelled.filter(F.col(self._UPDATE_KEY_COL) == "create").count()
        update_count = labelled.filter(F.col(self._UPDATE_KEY_COL) == "update").count()
        LoggingUtil().log_info(f"Curated PINS Inspector: {insert_count} new, {update_count} changed")

        end_exec_time = datetime.now()
        output_db, output_table_name = self.OUTPUT_TABLE.split(".", 1)
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": labelled,
                "storage_kind": "ADLSG2-Delta",
                "database_name": output_db,
                "table_name": output_table_name,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": output_table_name,
                "merge_keys": [key_col],
                "update_key_col": self._UPDATE_KEY_COL,
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=update_count,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )

    def write_data(self, data_to_write: Dict[str, Any]):
        """On first run create the table directly; subsequent runs delegate to SynapseDeltaIO merge."""
        if not self.spark.catalog.tableExists(self.OUTPUT_TABLE):
            df = data_to_write[self.OUTPUT_TABLE]["data"].drop(self._UPDATE_KEY_COL)
            df.write.format("delta").saveAsTable(self.OUTPUT_TABLE)
            LoggingUtil().log_info(f"Created curated table {self.OUTPUT_TABLE}")
            return
        super().write_data(data_to_write)
