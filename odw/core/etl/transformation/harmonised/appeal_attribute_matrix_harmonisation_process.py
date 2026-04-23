from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from typing import Dict


class AppealAttributeMatrixHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_attribute_matrix_harmonisation_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        df = self.spark.table("odw_standardised_db.appeal_attribute_matrix")
        return {"standardised_data": df}

    def process(self, **kwargs):
        start_exec_time = datetime.now()

        source_data = self.load_parameter("source_data", kwargs)
        df: DataFrame = self.load_parameter("standardised_data", source_data)

        std_cols = df.columns[:]

        for f in df.schema.fields:
            if isinstance(f.dataType, T.StringType):
                df = df.withColumn(f.name, F.trim(F.col(f.name)))

        df = df.withColumn("attribute", F.trim(F.lower(F.col("attribute"))))

        df = df.withColumn("TEMP_PK", F.sha2(F.to_json(F.struct(F.col("attribute"))), 256))

        if "ODTSourceSystem" not in df.columns:
            df = df.withColumn("ODTSourceSystem", F.lit("AppealAttributeMatrix"))

        if "IngestionDate" in df.columns:
            df = df.withColumn("IngestionDate", F.to_timestamp("IngestionDate"))
        else:
            df = df.withColumn("IngestionDate", F.current_timestamp())

        if "IsActive" not in df.columns:
            df = df.withColumn("IsActive", F.lit("Y"))

        extras = ["TEMP_PK", "ODTSourceSystem", "IngestionDate", "IsActive"]
        ordered_cols = [c for c in std_cols if c in df.columns] + extras
        df = df.select(*ordered_cols)

        if "s78" in df.columns:
            df = df.withColumn("s78", F.col("s78").cast("string"))

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "ref_appeal_attribute_matrix",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "ref_appeal_attribute_matrix",
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
