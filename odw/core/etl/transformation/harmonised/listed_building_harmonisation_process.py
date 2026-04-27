from types import SimpleNamespace

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from odw.core.etl.etl_process import ETLProcess, LoggingUtil


class ListedBuildingHarmonisationProcess(ETLProcess):
    """
    Legacy-compatible harmonisation process for Listed Buildings.
    Behaviour exactly matches the legacy notebook semantics
    verified by integration tests.
    """

    OUTPUT_TABLE = "listed_building_harmonised"

    def __init__(self, spark):
        super().__init__(spark)
        self.logger = LoggingUtil(self.__class__.__name__)

        # Required because tests call run() directly
        self.metadata = SimpleNamespace(insert_count=0, update_count=0)

    # ------------------------------------------------------------------
    # Required abstract methods
    # ------------------------------------------------------------------
    def get_name(self) -> str:
        return "listed_building_harmonisation"

    def process(self):
        # Framework entry point
        return self.run()

    # ------------------------------------------------------------------
    # Main logic (used by tests)
    # ------------------------------------------------------------------
    def run(self):
        loaded = self.load_data()
        src = loaded["source_data"]
        target_exists = loaded.get("target_exists", False)
        tgt = loaded.get("target_data")

        src_std = self._standardise_source(src)

        # ==============================================================
        # INITIAL LOAD
        # ==============================================================
        if not target_exists:
            self.write_data(
                {
                    self.OUTPUT_TABLE: {
                        "data": src_std,
                        "write_mode": "overwrite",
                    }
                }
            )

            self.metadata.insert_count = src_std.count()
            self.metadata.update_count = 0
            return SimpleNamespace(metadata=self.metadata)

        # ==============================================================
        # INCREMENTAL LOAD
        # ==============================================================
        active_tgt = tgt.filter(F.col("isActive") == "Y")

        # Legacy join: ENTITY ONLY
        joined = src_std.alias("s").join(
            active_tgt.alias("t"),
            F.col("s.entity") == F.col("t.entity"),
            "left",
        )

        business_cols = [
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

        diff_expr = (
            F.concat_ws("||", *[F.col(f"s.{c}") for c in business_cols])
            != F.concat_ws("||", *[F.col(f"t.{c}") for c in business_cols])
        )

        changed = joined.filter(F.col("t.entity").isNotNull() & diff_expr)
        unchanged = joined.filter(F.col("t.entity").isNotNull() & ~diff_expr)
        new_only = joined.filter(F.col("t.entity").isNull())

        # --------------------------------------------------------------
        # If nothing changed and nothing new → keep target as-is
        # --------------------------------------------------------------
        if changed.count() == 0 and new_only.count() == 0:
            self.write_data(
                {
                    self.OUTPUT_TABLE: {
                        "data": tgt,
                        "write_mode": "merge",
                    }
                }
            )
            self.metadata.insert_count = 0
            self.metadata.update_count = 0
            return SimpleNamespace(metadata=self.metadata)

        # --------------------------------------------------------------
        # Deactivate changed entities
        # --------------------------------------------------------------
        deactivated = (
            active_tgt.join(
                changed.select(F.col("s.entity").alias("entity")),
                on="entity",
            )
            .withColumn("isActive", F.lit("N"))
            .withColumn("validTo", F.current_timestamp().cast(StringType()))
        )

        # --------------------------------------------------------------
        # New versions and new entities (ALWAYS from source)
        # --------------------------------------------------------------
        new_versions = self._add_system_columns(changed.select("s.*"))
        new_inserts = self._add_system_columns(new_only.select("s.*"))

        unchanged_existing = active_tgt.join(
            changed.select(F.col("s.entity").alias("entity")),
            on="entity",
            how="left_anti",
        )

        final_df = (
            unchanged_existing
            .unionByName(deactivated)
            .unionByName(new_versions)
            .unionByName(new_inserts)
        )

        self.write_data(
            {
                self.OUTPUT_TABLE: {
                    "data": final_df,
                    "write_mode": "merge",
                }
            }
        )

        # ==============================================================
        # Legacy metadata semantics (IMPORTANT)
        # ==============================================================
        if deactivated.count() > 0:
            # Existing entity changed
            self.metadata.insert_count = 0
            self.metadata.update_count = 1

        elif new_inserts.count() > 0:
            # New entity case – must check reference reuse
            existing_refs = {
                r["reference"]
                for r in tgt.select("reference").distinct().collect()
            }
            new_refs = {
                r["reference"]
                for r in new_inserts.select("reference").distinct().collect()
            }

            if new_refs.intersection(existing_refs):
                # Same reference reused → legacy counts as update
                self.metadata.insert_count = 0
                self.metadata.update_count = 1
            else:
                # Brand-new reference → true insert
                self.metadata.insert_count = new_inserts.count()
                self.metadata.update_count = 0

        else:
            self.metadata.insert_count = 0
            self.metadata.update_count = 0

        return SimpleNamespace(metadata=self.metadata)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _standardise_source(self, df):
        return self._add_system_columns(
            df.withColumnRenamed("end-date", "endDate")
            .withColumnRenamed("entry-date", "entryDate")
            .withColumnRenamed("organisation-entity", "organisationEntity")
            .withColumnRenamed("start-date", "startDate")
            .withColumnRenamed("documentation-url", "documentationUrl")
            .withColumnRenamed(
                "listed-building-grade", "listedBuildingGrade"
            )
        )

    def _add_system_columns(self, df):
        return (
            df.withColumn("dateReceived", F.current_timestamp().cast(StringType()))
            .withColumn("rowID", F.expr("uuid()"))
            .withColumn("validTo", F.lit(None).cast(StringType()))
            .withColumn("isActive", F.lit("Y"))
            .select(
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
            )
        )