from typing import Dict, Tuple
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil


class AppealAttributeMatrixCuratedProcess(CurationProcess):
    STANDARDISED_TABLE = "odw_standardised_db.appeal_attribute_matrix"
    HARMONISED_TABLE = "odw_harmonised_db.ref_appeal_attribute_matrix"
    OUTPUT_TABLE = "odw_curated_db.ref_appeal_attribute_matrix"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_attribute_matrix_curated_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info("Loading harmonised and standardised Appeal Attribute Matrix data")

        harmonised_df = self.spark.table(self.HARMONISED_TABLE)
        standardised_df = self.spark.table(self.STANDARDISED_TABLE)

        return {"harmonised_data": harmonised_df, "standardised_schema": standardised_df}

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        df_h: DataFrame = source_data["harmonised_data"]
        df_std: DataFrame = source_data["standardised_schema"]

        std_cols = df_std.columns

        if "IsActive" in df_h.columns:
            df = df_h.filter(F.col("IsActive") == "Y")

        elif {"TEMP_PK", "IngestionDate"}.issubset(set(df_h.columns)):
            latest = df_h.groupBy("TEMP_PK").agg(F.max("IngestionDate").alias("IngestionDate"))
            df = df_h.join(latest, on=["TEMP_PK", "IngestionDate"], how="inner")

        else:
            df = df_h

        for c in std_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))

        extras = [c for c in df.columns if c not in std_cols]
        ordered_cols = std_cols + extras
        df = df.select(*[F.col(c) for c in ordered_cols])

        if "s78" in df.columns:
            df = df.withColumn("s78", F.col("s78").cast(T.StringType()))

        insert_count = df.count()

        LoggingUtil().log_info(f"Curated Appeal Attribute Matrix row count: {insert_count}")

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "ref_appeal_attribute_matrix",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "AppealAttributeMatrix/appeal_attribute_matrix",
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
                activity_type=self.get_name(),
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
