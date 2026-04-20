from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult

from datetime import datetime
from typing import Dict

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import md5, concat, coalesce, lit, col


# ------------------------------------------------------------------ #
# Columns used to build RowID hash
# ------------------------------------------------------------------ #
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
    ETL process for harmonising Listed Building data
    from the standardised layer to the harmonised layer.
    """

    @staticmethod
    def get_name() -> str:
        """
        Name used by ETLProcessFactory
        """
        return "listed-buildings-harmonisation"

    def __init__(self, spark: SparkSession, params: Dict):
        super().__init__(spark, params)

        # ✅ CORRECT logger initialisation
        self.logging_util = LoggingUtil()

        self.source_table = "odw_standardised_db.listed_building"
        self.target_table = "odw_harmonised_db.listed_building"

        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0

    # ------------------------------------------------------------------ #
    # Step 1: Read
    # ------------------------------------------------------------------ #
    def read_source(self) -> DataFrame:
        self.logging_util.log_info(f"Reading source table {self.source_table}")

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

        self.logging_util.log_info(f"Source row count: {df.count()}")
        return df

    # ------------------------------------------------------------------ #
    # Step 2: Transform
    # ------------------------------------------------------------------ #
    def transform(self, df: DataFrame) -> DataFrame:
        self.logging_util.log_info("Transforming source data")

        hash_expr = concat(*[
            coalesce(col(c), lit("."))
            for c in _DOCUMENT_ROW_ID_COLUMNS
        ])

        return (
            df
            .withColumn("rowID", md5(hash_expr))
            .withColumn("validTo", lit(None).cast("timestamp"))
            .withColumn("isActive", lit("Y"))
        )

    # ------------------------------------------------------------------ #
    # Step 3: Merge
    # ------------------------------------------------------------------ #
    def merge(self, df: DataFrame):
        if not self.spark.catalog.tableExists(
            "odw_harmonised_db", "listed_building"
        ):
            self.logging_util.log_info(
                "Target table does not exist – performing initial load"
            )
            self._initial_load(df)
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

        # Insert new or changed records
        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.entity = s.entity AND t.rowID = s.rowID AND t.isActive = 'Y'"
            )
            .whenNotMatchedInsert(
                values={
                    "dataset": "s.dataset",
                    "endDate": "s.endDate",
                    "entity": "s.entity",
                    "entryDate": "s.entryDate",
                    "geometry": "s.geometry",
                    "listedBuildingGrade": "s.listedBuildingGrade",
                    "name": "s.name",
                    "organisationEntity": "s.organisationEntity",
                    "point": "s.point",
                    "prefix": "s.prefix",
                    "reference": "s.reference",
                    "startDate": "s.startDate",
                    "typology": "s.typology",
                    "documentationUrl": "s.documentationUrl",
                    "dateReceived": "s.dateReceived",
                    "rowID": "s.rowID",
                    "validTo": "s.validTo",
                    "isActive": "s.isActive",
                }
            )
            .execute()
        )

        self._calculate_counts(df, delta_table)

    # ------------------------------------------------------------------ #
    # Helper methods
    # ------------------------------------------------------------------ #
    def _initial_load(self, df: DataFrame):
        df.write.format("delta").mode("overwrite").saveAsTable(self.target_table)
        self.insert_count = df.count()
        self.update_count = 0

    def _calculate_counts(self, df: DataFrame, delta_table: DeltaTable):
        active_df = delta_table.toDF().filter("isActive = 'Y'")

        self.insert_count = df.join(
            active_df, "reference", "left_anti"
        ).count()

        self.update_count = (
            df.join(active_df, "reference", "inner")
            .filter(df.rowID != active_df.rowID)
            .count()
        )

        self.logging_util.log_info(
            f"Insert count: {self.insert_count}, "
            f"Update count: {self.update_count}"
        )

    # ------------------------------------------------------------------ #
    # Framework-required entry point
    # ------------------------------------------------------------------ #
    def process(self) -> ETLResult:
        """
        Required abstract method from HarmonisationProcess.
        Delegates to execute().
        """
        return self.execute()

    # ------------------------------------------------------------------ #
    # Execute (implementation)
    # ------------------------------------------------------------------ #
    def execute(self) -> ETLResult:
        start_time = datetime.utcnow()

        src_df = self.read_source()
        final_df = self.transform(src_df)
        self.merge(final_df)

        return ETLSuccessResult(
            start_time=start_time,
            end_time=datetime.utcnow(),
            rows_inserted=self.insert_count,
            rows_updated=self.update_count,
            rows_deleted=self.delete_count,
        )
