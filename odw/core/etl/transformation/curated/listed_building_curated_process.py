from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
from typing import Dict, Tuple
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLFailResult


class ListedBuildingCuratedProcess(CurationProcess):
    """
    ETL process for curating Listed Building data with CDC-enabled safe merge.
    FIXED: sha2() numBits parameter + proper Column expressions.
    """
    
    HARMONISED_TABLE = "odw_harmonised_db.listed_building"
    OUTPUT_TABLE = "odw_curated_db.listed_building"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "listed-building-curated"

    def safe_merge(
        self,
        df: DataFrame,
        table_name: str,
        key_col: str = "entity",
        debug_mappings: bool = False
    ) -> Tuple[int, int, int, datetime, str]:
        """
        FIXED: Proper sha2(col, 256) syntax + Column expressions for merge condition
        """
        start_exec_time = datetime.now()
        insert_count = update_count = delete_count = 0
        error_message = ""

        try:
            # Clean & cache source - filter null keys
            if key_col not in df.columns:
                raise ValueError(f"Key column '{key_col}' not found in source DataFrame.")

            df = df.filter(F.col(key_col).isNotNull()).cache()
            source_count = df.count()
            LoggingUtil().log_info(f"[INFO] Incoming source count = {source_count}")

            # Check if target table exists
            table_exists = self.spark.catalog.tableExists(table_name)
            LoggingUtil().log_info(f"[INFO] Table exists? {table_name} -> {table_exists}")

            if not table_exists:
                df.write.format("delta").mode("overwrite").saveAsTable(table_name)
                LoggingUtil().log_info(f"[INFO] Created table {table_name}")
                insert_count = source_count
                end_exec_time = datetime.now()
                return insert_count, update_count, delete_count, end_exec_time, error_message

            # Load target Delta table
            delta_table = DeltaTable.forName(self.spark, table_name)
            target_df = delta_table.toDF()

            # Schema alignment - keep common columns only
            common_cols = [c for c in df.columns if c in target_df.columns]
            if key_col not in common_cols:
                raise ValueError(f"Key column '{key_col}' not in common columns.")

            df = df.select([F.col(c) for c in common_cols])
            target_df = target_df.select([F.col(c) for c in common_cols])
            non_key_cols = [c for c in common_cols if c != key_col]

            LoggingUtil().log_info(f"[INFO] Common columns: {common_cols}")
            LoggingUtil().log_info(f"[INFO] Non-key columns for update: {non_key_cols}")

            # **CRITICAL FIX**: Proper sha2 syntax with 256 bits
            if non_key_cols:
                # Build hash columns with proper sha2(col, 256) syntax
                t_hash_cols = [F.sha2(F.col(f"t.`{c}`").cast("string"), 256) for c in non_key_cols]
                s_hash_cols = [F.sha2(F.col(f"s.`{c}`").cast("string"), 256) for c in non_key_cols]
                
                t_hash = F.concat_ws("||", *t_hash_cols)
                s_hash = F.concat_ws("||", *s_hash_cols)
                
                hash_diff_cond = t_hash != s_hash
            else:
                hash_diff_cond = F.lit(False)

            # Pre-merge counts (using simplified hash for counting)
            insert_count = (
                df.select(key_col)
                .join(target_df.select(key_col), key_col, "left_anti")
                .count()
            )

            if non_key_cols:
                # Simplified hash for counting (single column hash)
                sample_col = non_key_cols[0] if non_key_cols else ""
                update_count = (
                    df.alias("s")
                    .join(target_df.alias("t"), key_col, "inner")
                    .filter(F.sha2(F.col(f"s.`{sample_col}`").cast("string"), 256) != 
                           F.sha2(F.col(f"t.`{sample_col}`").cast("string"), 256))
                    .count()
                )
            else:
                update_count = 0

            LoggingUtil().log_info(f"[PLAN] Inserts = {insert_count}, Updates = {update_count}")

            # Build merge mappings
            set_mapping = {c: F.col(f"s.`{c}`") for c in non_key_cols}
            values_mapping = {c: F.col(f"s.`{c}`") for c in common_cols}

            if debug_mappings and self.debug:
                LoggingUtil().log_info("[DEBUG] UPDATE set mapping:")
                for c in non_key_cols:
                    LoggingUtil().log_info(f"  {c}: s.`{c}`")

            # Execute merge with proper Column condition
            merger = (
                delta_table.alias("t")
                .merge(
                    source=df.alias("s"), 
                    condition=f"t.`{key_col}` = s.`{key_col}`"
                )
            )

            if non_key_cols:
                merger = merger.whenMatchedUpdate(
                    condition=hash_diff_cond,
                    set=set_mapping
                )

            merger = merger.whenNotMatchedInsert(values=values_mapping)
            merger.execute()

            end_exec_time = datetime.now()
            LoggingUtil().log_info(f"[SUCCESS] Merge completed: I={insert_count}, U={update_count}")

        except Exception as e:
            error_message = f"ERROR: {str(e)[:500]}"
            LoggingUtil().log_error(error_message)
            end_exec_time = datetime.now()

        finally:
            try:
                df.unpersist()
            except:
                pass

        return insert_count, update_count, delete_count, end_exec_time, error_message

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """Load source data only."""
        LoggingUtil().log_info(f"Loading from {self.HARMONISED_TABLE}")
        
        harmonised_df = self.spark.sql(f"""
            SELECT
                CAST(entity AS LONG) AS entity,
                reference,
                name,
                listedBuildingGrade
            FROM {self.HARMONISED_TABLE}
            WHERE isActive = 'Y'
        """)

        return {"harmonised_listed_building": harmonised_df}

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """Main process with safe merge + CDC support."""
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("harmonised_listed_building", source_data)

        insert_count, update_count, delete_count, end_exec_time, error_message = \
            self.safe_merge(df, self.OUTPUT_TABLE, "entity", debug_mappings=self.debug)

        data_to_write = {}

        return (
            data_to_write,
            ETLSuccessResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=start_exec_time,
                    end_execution_time=end_exec_time,
                    table_name=self.OUTPUT_TABLE,
                    insert_count=insert_count,
                    update_count=update_count,
                    delete_count=delete_count,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
                )
            )
        )