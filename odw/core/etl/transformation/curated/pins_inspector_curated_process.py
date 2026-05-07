from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Any, Dict, Tuple
from datetime import datetime


class PinsInspectorCuratedProcess(CurationProcess):
    """
    ETL process for curating the PINS Inspector dataset into odw_curated_db.

    Reads active inspectors from the harmonised layer and upserts them into
    the curated Delta table using hash-based change detection.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "pins-inspector-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.pins_inspector"
    OUTPUT_TABLE = "odw_curated_db.pins_inspector"
    _KEY_COL = "entraId"
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
        return "pins-inspector-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading active inspectors from {self.HARMONISED_TABLE}")
        harmonised = self.spark.sql(f"""
            SELECT DISTINCT
                {", ".join(self._OUTPUT_COLUMNS)}
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)
        return {"harmonised_inspector": harmonised}

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("harmonised_inspector", source_data)

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated PINS Inspector source row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "pins_inspector",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "pins_inspector",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }
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

    def write_data(self, data_to_write: Dict[str, Any]):
        """Delta MERGE upsert — updates changed inspectors, inserts new ones."""
        from delta.tables import DeltaTable

        table_metadata = data_to_write[self.OUTPUT_TABLE]
        df: DataFrame = table_metadata["data"]
        key_col = self._KEY_COL

        df = df.filter(F.col(key_col).isNotNull()).cache()

        if not self.spark.catalog.tableExists(self.OUTPUT_TABLE):
            df.write.format("delta").mode("errorifexists").saveAsTable(self.OUTPUT_TABLE)
            LoggingUtil().log_info(f"Created curated table {self.OUTPUT_TABLE}")
            df.unpersist()
            return

        delta_table = DeltaTable.forName(self.spark, self.OUTPUT_TABLE)
        target_df = delta_table.toDF()

        # Align to the intersection of source and target schemas
        common_cols = [c for c in df.columns if c in target_df.columns]
        df = df.select(common_cols)
        non_key_cols = [c for c in common_cols if c != key_col]

        t_struct = ",".join([f"'`{c}`', t.`{c}`" for c in non_key_cols])
        s_struct = ",".join([f"'`{c}`', s.`{c}`" for c in non_key_cols])
        hash_diff_cond = (
            f"sha2(to_json(named_struct({t_struct})), 256) "
            f"<> sha2(to_json(named_struct({s_struct})), 256)"
        )

        (
            delta_table.alias("t")
            .merge(df.alias("s"), f"t.`{key_col}` = s.`{key_col}`")
            .whenMatchedUpdate(condition=hash_diff_cond, set={c: F.col(f"s.`{c}`") for c in non_key_cols})
            .whenNotMatchedInsert(values={c: F.col(f"s.`{c}`") for c in common_cols})
            .execute()
        )

        LoggingUtil().log_info(f"Merge complete into {self.OUTPUT_TABLE}")
        df.unpersist()
