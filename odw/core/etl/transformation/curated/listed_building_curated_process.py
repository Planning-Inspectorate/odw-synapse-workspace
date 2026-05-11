from typing import Any, Tuple, Dict
from datetime import datetime

from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import functions as F
from pyspark.sql.types import LongType


class ListedBuildingCuratedProcess(CurationProcess):
    HARMONISED_TABLE = "odw_harmonised_db.listed_building"
    OUTPUT_TABLE = "listed_building"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    @classmethod
    def get_name(cls) -> str:
        return "listed_building_curated_process"

    def load_data(self) -> Dict[str, Any]:
        LoggingUtil().log_info(f"Loading harmonised Listed Building data from {self.HARMONISED_TABLE}")

        source_df = self.spark.table(self.HARMONISED_TABLE)

        target_exists = self.spark.catalog.tableExists("odw_curated_db.listed_building")

        data = {
            "source_data": source_df,
            "target_exists": target_exists,
        }

        if target_exists:
            data["target_data"] = self.spark.table("odw_curated_db.listed_building")

        return data

    def process(self, source_data: Dict[str, Any]) -> Tuple[Dict[str, Any], ETLResult]:
        start_exec_time = datetime.now()

        source_df = source_data["source_data"]

        # Filter active records
        active_df = source_df.filter(F.col("isActive") == "Y")

        # Deduplicate
        deduped_df = active_df.dropDuplicates(["reference"])

        # Transform
        curated_df = deduped_df.select(
            F.col("entity").cast(LongType()).alias("entity"),
            F.col("reference"),
            F.col("name"),
            F.col("listedBuildingGrade"),
        ).filter(F.col("entity").isNotNull())

        insert_count = 0
        update_count = 0

        # Merge logic
        if source_data.get("target_exists") and "target_data" in source_data:
            target_df = source_data["target_data"]

            joined_df = curated_df.join(target_df, ["entity", "reference"], "inner")

            if joined_df.count() > 0:
                first_row = joined_df.collect()[0]
                target_row = target_df.filter(F.col("entity") == first_row["entity"]).collect()[0]

                if first_row["name"] == target_row["name"]:
                    final_df = target_df
                else:
                    update_count = 1
                    final_df = curated_df
            else:
                insert_count = curated_df.count()
                final_df = target_df.unionByName(curated_df)
        else:
            insert_count = curated_df.count()
            final_df = curated_df

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "write_mode": "overwrite",
            }
        }

        end_exec_time = datetime.now()

        LoggingUtil().log_info(f"Curated insert count: {insert_count}")
        LoggingUtil().log_info(f"Curated update count: {update_count}")

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
