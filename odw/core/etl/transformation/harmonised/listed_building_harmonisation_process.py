from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLFailResult

from datetime import datetime
from typing import Dict
import traceback

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import md5, concat, coalesce, lit, col


# ------------------------------------------------------------------
# Columns used to build RowID hash
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
    to the harmonised layer.
    """

    @staticmethod
    def get_name() -> str:
        return "listed-buildings-harmonisation"

    def __init__(self, spark: SparkSession, params: Dict):
        super().__init__(spark, params)

        self.logging_util = LoggingUtil()

        self.source_table = "odw_standardised_db.listed_building"
        self.target_table = "odw_harmonised_db.listed_building"

        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0

    # ✅ REQUIRED abstract method
    def process(self, **kwargs):
        return self.execute()

    # ✅ Override run to bypass load_data
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

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        hash_expr = concat(
            *[coalesce(col(c), lit(".")) for c in _DOCUMENT_ROW_ID_COLUMNS]
        )

        return (
            df
            .withColumn("rowID", md5(hash_expr))
            .withColumn("validTo", lit(None).cast("timestamp"))
            .withColumn("isActive", lit("Y"))
        )

    def merge(self, df: DataFrame):
        if not self.spark.catalog.tableExists(
            "odw_harmonised_db", "listed_building"
        ):
            df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(self.target_table)
            self.insert_count = df.count()
            return

        delta_table = DeltaTable.forName(self.spark, self.target_table)

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

        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.entity = s.entity AND t.rowID = s.rowID AND t.isActive = 'Y'"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def execute(self) -> ETLResult:
        start_time = datetime.utcnow()
        df = self.transform(self.read_source())
        self.merge(df)

        return ETLSuccessResult(
            start_time=start_time,
            end_time=datetime.utcnow(),
            rows_inserted=self.insert_count,
            rows_updated=self.update_count,
            rows_deleted=self.delete_count,
        )
