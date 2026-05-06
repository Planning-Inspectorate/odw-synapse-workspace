from datetime import datetime
from typing import Dict, Tuple, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.logging_util import LoggingUtil


class ListedBuildingHarmonisationProcess(HarmonisationProcess):
    SOURCE_TABLE = "odw_standardised_db.listed_building"
    OUTPUT_TABLE = "odw_harmonised_db.listed_building"

    _ROW_ID_COLUMNS = [
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

    _OUTPUT_COLUMNS = [
        "dataset",
        "endDate",
        "entity",
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
        "dateReceived",
        "rowID",
        "validTo",
        "isActive",
    ]

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

        try:
            self.spark.catalog.refreshTable(self.SOURCE_TABLE)
        except Exception:
            pass

    @classmethod
    def get_name(cls) -> str:
        return "listed_building_harmonisation_process"

    # ✅ SAFE LOGGER (no pipeline break risk)
    def _safe_log_info(self, message: str) -> None:
        try:
            LoggingUtil().log_info(message)
        except Exception:
            pass

    def _rename_source(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumnRenamed("end-date", "endDate")
            .withColumnRenamed("entry-date", "entryDate")
            .withColumnRenamed("listed-building-grade", "listedBuildingGrade")
            .withColumnRenamed("organisation-entity", "organisationEntity")
            .withColumnRenamed("start-date", "startDate")
            .withColumnRenamed("documentation-url", "documentationUrl")
        )

    def _add_harmonised_fields(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("dateReceived", F.current_date())
            .withColumn(
                "rowID",
                F.md5(F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in self._ROW_ID_COLUMNS])),
            )
            .withColumn("validTo", F.lit(None).cast(TimestampType()))
            .withColumn("isActive", F.lit("Y"))
        )

    def process(self, **kwargs) -> Tuple[Dict[str, Dict[str, Any]], ETLResult]:
        start_exec_time = datetime.now()

        # ✅ Logging start
        self._safe_log_info("Starting listed_building harmonisation process")

        injected = kwargs.get("source_data")
        source_df = injected.get("source_data")
        target_df = injected.get("target_data")
        target_exists = injected.get("target_exists", False)

        staged_df = self._add_harmonised_fields(self._rename_source(source_df))

        # ✅ Initial load
        if not target_exists or target_df is None:
            result = self._result(
                staged_df,
                start_exec_time,
                insert_count=staged_df.count(),
                update_count=0,
            )

            self._safe_log_info("Initial load completed")
            return result

        active_target = target_df.filter(F.col("isActive") == "Y")

        # ✅ ENTITY + REFERENCE join
        joined = staged_df.alias("src").join(
            active_target.alias("tgt"),
            (F.col("src.reference") == F.col("tgt.reference")) & (F.col("src.entity") == F.col("tgt.entity")),
            "left",
        )

        # ✅ Changed rows
        changed = joined.filter(
            F.col("tgt.rowID").isNotNull()
            & (
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(f"src.{c}"), F.lit("##NULL##")) for c in self._ROW_ID_COLUMNS],
                )
                != F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(f"tgt.{c}"), F.lit("##NULL##")) for c in self._ROW_ID_COLUMNS],
                )
            )
        )

        # ✅ Candidate new rows
        candidate_new = joined.filter(F.col("tgt.rowID").isNull()).select("src.*")

        # ✅ Prevent duplicate identical active rows
        existing_active_business = active_target.select(self._ROW_ID_COLUMNS).distinct()

        new_inserts = candidate_new.alias("n").join(
            existing_active_business.alias("e"),
            [F.col(f"n.{c}").eqNullSafe(F.col(f"e.{c}")) for c in self._ROW_ID_COLUMNS],
            "left_anti",
        )

        # ✅ Expire old rows
        expired = changed.select("tgt.*").withColumn("validTo", F.current_timestamp()).withColumn("isActive", F.lit("N"))

        # ✅ New versions
        new_versions = changed.select("src.*")

        # ✅ Preserve existing rows
        changed_refs = [r["reference"] for r in changed.select("tgt.reference").distinct().collect()]

        preserved_target = target_df.filter((F.col("isActive") == "N") | (~F.col("reference").isin(changed_refs)))

        final_df = preserved_target.unionByName(expired).unionByName(new_versions).unionByName(new_inserts)

        # ✅ INSERT count
        existing_refs = target_df.select("reference").distinct()

        true_inserts = new_inserts.alias("n").join(existing_refs.alias("e"), "reference", "left_anti")

        # ✅ UPDATE count
        reference_reuse_updates = new_inserts.alias("n").join(existing_refs.alias("e"), "reference", "inner")

        result = self._result(
            final_df,
            start_exec_time,
            insert_count=true_inserts.count(),
            update_count=changed.count() + reference_reuse_updates.count(),
        )

        # ✅ Logging end
        self._safe_log_info("Completed listed_building harmonisation process")

        return result

    def _result(
        self,
        df: DataFrame,
        start_exec_time: datetime,
        insert_count: int,
        update_count: int,
    ) -> Tuple[Dict[str, Dict[str, Any]], ETLResult]:

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df.select(*self._OUTPUT_COLUMNS),
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "listed_building",
                "storage_endpoint": "mock-storage-account",
                "container_name": "odw-harmonised",
                "blob_path": "listed_building",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["isActive"],
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
