from datetime import datetime
from typing import Dict
import traceback

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLFailResult
from odw.core.util.logging_util import LoggingUtil


# ------------------------------------------------------------------
# Columns used to build deterministic RowID hash (CDC)
# ------------------------------------------------------------------
_DOCUMENT_ROW_ID_COLUMNS = [
    "entity",
    "dataset",
    "endDate",
    "entryDate",
    "geometry",
    "listedBuildingGrade",
    "name",
    "organisationEntity",
    "point",
    "prefix",
    "reference",
    "startDate",
    "typology",
    "documentationUrl",
]


class ListedBuildingHarmonisationProcess(HarmonisationProcess):
    """
    Harmonises Listed Building data from the standardised layer
    to the harmonised layer using SCD Type‑2 semantics.
    """

    # ------------------------------------------------------------------
    # Factory name
    # ------------------------------------------------------------------
    @staticmethod
    def get_name() -> str:
        return "listed-buildings-harmonisation"

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(self, spark: SparkSession, params: Dict):
        super().__init__(spark, params)

        self.spark = spark
        self.logging_util = LoggingUtil()

        self.source_table = "odw_standardised_db.listed_building"
        self.target_table = "odw_harmonised_db.listed_building"

        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0

    # ------------------------------------------------------------------
    # REQUIRED abstract method (framework)
    # ------------------------------------------------------------------
    def process(self, **kwargs):
        return self.execute()

    # ------------------------------------------------------------------
    # Override run() to bypass load_data()
    # ------------------------------------------------------------------
    def run(self, **kwargs) -> ETLResult:
        start_time = datetime.utcnow()
        try:
            self.logging_util.log_info(
                "Starting ListedBuildingHarmonisationProcess"
            )
            result = self.execute()
            self.logging_util.log_info(result)
            return result

        except Exception as e:
            self.logging_util.log_error(str(e))
            self.logging_util.log_error(traceback.format_exc())
            return ETLFailResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=start_time,
                    end_execution_time=datetime.utcnow(),
                    exception=str(e),
                    exception_trace=traceback.format_exc(),
                    table_name=self.target_table,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
                    insert_count=0,
                    update_count=0,
                    delete_count=0,
                )
            )

    # ------------------------------------------------------------------
    # Step 1: Read source
    # ------------------------------------------------------------------
    def read_source(self) -> DataFrame:
        df = self.spark.sql(f"""
            SELECT
                dataset,
                `end-date` AS endDate,
                entity,
                `entry-date` AS entryDate,
                geometry,
                `listed-building-grade` AS listedBuildingGrade,
                name,
                `organisation-entity` AS organisationEntity,
                point,
                prefix,
                reference,
                `start-date` AS startDate,
                typology,
                `documentation-url` AS documentationUrl
            FROM {self.source_table}
        """).withColumn(
            "dateReceived", F.current_date()
        )

        self.logging_util.log_info(
            f"Source row count: {df.count()}"
        )
        return df

    # ------------------------------------------------------------------
    # Step 2: Transform (FIXED md5 logic)
    # ------------------------------------------------------------------
    def transform(self, df: DataFrame) -> DataFrame:
        hash_expr = F.md5(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c), F.lit("")) for c in _DOCUMENT_ROW_ID_COLUMNS]
            )
        )

        return (
            df
            .withColumn("rowID", hash_expr)
            .withColumn("validTo", F.lit(None).cast("timestamp"))
            .withColumn("isActive", F.lit("Y"))
        )

    # ------------------------------------------------------------------
    # Step 3: Merge (CDC)
    # ------------------------------------------------------------------
    def merge(self, df: DataFrame):
        if not self.spark.catalog.tableExists(
            "odw_harmonised_db", "listed_building"
        ):
            self.logging_util.log_info(
                "Target does not exist – performing initial load"
            )
            df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(self.target_table)

            self.insert_count = df.count()
            return

        delta_table = DeltaTable.forName(self.spark, self.target_table)

        # Deactivate changed records
        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.entity = s.entity AND t.isActive = 'Y'"
            )
            .whenMatchedUpdate(
                condition="t.rowID <> s.rowID",
                set={
                    "validTo": "current_date()",
                    "isActive": "'N'"
                }
            )
            .execute()
        )

        # Insert new / changed records
        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.entity = s.entity AND t.rowID = s.rowID AND t.isActive = 'Y'"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        self._calculate_counts(df, delta_table)

    # ------------------------------------------------------------------
    # Counts
    # ------------------------------------------------------------------
    def _calculate_counts(self, df: DataFrame, delta_table: DeltaTable):
        active_df = delta_table.toDF().filter("isActive = 'Y'")

        self.insert_count = (
            df.join(active_df, "reference", "left_anti").count()
        )

        self.update_count = (
            df.join(active_df, "reference")
              .filter(df.rowID != active_df.rowID)
              .count()
        )

        self.logging_util.log_info(
            f"Insert count: {self.insert_count}, "
            f"Update count: {self.update_count}"
        )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    def execute(self) -> ETLResult:
        start_time = datetime.utcnow()

        df = self.read_source()
        df = self.transform(df)
        self.merge(df)

        return ETLSuccessResult(
            start_time=start_time,
            end_time=datetime.utcnow(),
            rows_inserted=self.insert_count,
            rows_updated=self.update_count,
            rows_deleted=self.delete_count,
        )