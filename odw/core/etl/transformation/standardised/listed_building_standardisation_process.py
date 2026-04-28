from datetime import datetime
from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

from odw.core.etl.transformation.standardised.standardisation_process import (
    StandardisationProcess,
)
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util

try:
    from notebookutils import mssparkutils
except Exception:
    mssparkutils = None


class ListedBuildingStandardisationProcess(StandardisationProcess):
    """
    Standardisation process for Listed Buildings and Listed Building Outline.
    Converts raw JSON input into standardised Delta tables.
    """

    @classmethod
    def get_name(cls) -> str:
        return "listed-buildings-standardisation"

    def run(self, **kwargs) -> Any:
        """
        Orchestrates load → process → write.

        Test / legacy compatibility:
        - Tests mock load_data() and return RAW dataframes
        - Keys must be mapped before calling process()
        - write_data() must ALWAYS be called
        """
        raw_source_data = self.load_data(**kwargs)

        source_data = {
            "listed_building": raw_source_data.get("listed_building_data"),
            "listed_building_outline": raw_source_data.get("listed_building_outline_data"),
        }

        outputs, result = self.process(source_data=source_data)

        self.write_data(outputs)

        return result

    @staticmethod
    def get_listed_building_schema() -> StructType:
        return StructType([
            StructField("dataset", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("name", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("listed-building-grade", StringType(), True),
        ])

    @staticmethod
    def get_listed_building_outline_schema() -> StructType:
        return StructType([
            StructField("address", StringType(), True),
            StructField("address-text", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("document-url", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("listed-building", StringType(), True),
            StructField("name", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
        ])

    def _get_date_folder(self, kwargs: Dict) -> str:
        date_folder = kwargs.get("date_folder") or kwargs.get("run_date")

        if not date_folder and mssparkutils is not None:
            try:
                date_folder = mssparkutils.notebook.getArgument("date_folder")
            except Exception:
                pass

        if not date_folder and mssparkutils is not None:
            try:
                date_folder = mssparkutils.notebook.getArgument("run_date")
            except Exception:
                pass

        if not date_folder:
            raise ValueError("load_data requires 'date_folder' parameter")

        return date_folder

    @LoggingUtil.logging_to_appins
    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        date_folder = self._get_date_folder(kwargs)
        storage_account = Util.get_storage_account()

        base_path = f"abfss://odw-raw@{storage_account}/ListedBuildings/{date_folder}"
        lb_path = f"{base_path}/listed_building.json"
        lbo_path = f"{base_path}/listed_building_outline.json"

        LoggingUtil().log_info(f"Reading Listed Buildings from {lb_path}")
        LoggingUtil().log_info(f"Reading Listed Building Outline from {lbo_path}")

        listed_buildings_raw = self.spark.read.option("multiline", "true").json(lb_path)
        listed_building_outline_raw = self.spark.read.option("multiline", "true").json(lbo_path)

        listed_buildings_df = (
            listed_buildings_raw
            .selectExpr("explode(entities) as entity")
            .select("entity.*")
        )

        listed_building_outline_df = (
            listed_building_outline_raw
            .selectExpr("explode(entities) as entity")
            .select("entity.*")
        )

        return {
            "listed_building_data": listed_buildings_df,
            "listed_building_outline_data": listed_building_outline_df,
        }

    def _flatten_entities(self, df: DataFrame) -> DataFrame:
        if df is None:
            return None

        if "entities" in df.columns:
            return df.selectExpr("explode(entities) as entity").select("entity.*")

        return df

    @LoggingUtil.logging_to_appins
    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = kwargs.get("source_data")
        if not source_data:
            raise ValueError("process requires source_data dictionary")

        lb_df = self._flatten_entities(source_data.get("listed_building"))
        lbo_df = self._flatten_entities(source_data.get("listed_building_outline"))

        if lb_df is None or lbo_df is None:
            raise ValueError("Required Listed Buildings dataframes are missing")

        lb_table = "listed_building"
        lbo_table = "listed_building_outline"

        insert_count = lb_df.count() + lbo_df.count()
        end_exec_time = datetime.now()

        return (
            {
                lb_table: {
                    "data": lb_df,
                    "write_mode": "overwrite",
                },
                lbo_table: {
                    "data": lbo_df,
                    "write_mode": "overwrite",
                },
            },
            ETLSuccessResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=start_exec_time,
                    end_execution_time=end_exec_time,
                    table_name="listed_building_and_listed_building_outline",
                    insert_count=insert_count,
                    update_count=0,
                    delete_count=0,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
                )
            ),
        )