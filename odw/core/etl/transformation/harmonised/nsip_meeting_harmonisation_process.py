from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Business key columns for SCD2
_BUSINESS_KEY_COLS = ["NSIPProjectInfoInternalID", "caseId", "meetingId"]

# Change tracking columns (combined with business key for row_hash)
_CHANGE_TRACKING_COLS = [
    "meetingAgenda",
    "planningInspectorateRole",
    "meetingType",
    "meetingDate",
    "estimatedPrelimMeetingDate",
]


def _align_source_to_target_schema(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    """
    Keep all source columns:
    - Columns in both source and target: Cast to target datatype
    - Columns only in source (not in target): Keep with original datatype
    - Columns only in target (not in source): Ignored
    """
    target_schema = {field.name: field.dataType for field in target_df.schema.fields}
    aligned_columns = []
    for col_name in source_df.columns:
        if col_name in target_schema:
            aligned_columns.append(F.col(col_name).cast(target_schema[col_name]).alias(col_name))
        else:
            aligned_columns.append(F.col(col_name))
    return source_df.select(aligned_columns)


class NsipMeetingHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Meeting data using SCD Type 2 logic.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-meeting-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_project"
    OUTPUT_TABLE = "odw_harmonised_db.sb_nsip_meeting"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-meeting-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading meetings from {self.SERVICE_BUS_TABLE}")

        service_bus_data = self.spark.sql(f"""
            SELECT
                NSIPProjectInfoInternalID
                ,caseId
                ,caseReference
                ,meetings
                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
            FROM
                {self.SERVICE_BUS_TABLE}
            WHERE
                meetings IS NOT NULL
        """)

        # Ensure target table exists and read it
        LoggingUtil().log_info(f"Loading target table {self.OUTPUT_TABLE}")
        if self.spark.catalog.tableExists(self.OUTPUT_TABLE):
            target_df = self.spark.table(self.OUTPUT_TABLE)
        else:
            target_df = None

        return {
            "service_bus_data": service_bus_data,
            "target_df": target_df,
        }

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        target_df = source_data.get("target_df")

        # Step 1: Explode meetings array
        LoggingUtil().log_info("Exploding meetings array")
        service_bus_exploded = service_bus_data.select(
            F.col("NSIPProjectInfoInternalID"),
            F.col("caseId"),
            F.col("caseReference"),
            F.explode(F.col("meetings")).alias("meeting"),
            F.col("Migrated"),
            F.col("ODTSourceSystem"),
            F.col("SourceSystemID"),
            F.col("IngestionDate"),
        )

        # Step 2: Flatten nested meeting columns
        service_bus_exploded = service_bus_exploded.select(
            "NSIPProjectInfoInternalID",
            "caseId",
            "caseReference",
            F.col("meeting.meetingId").alias("meetingId"),
            F.col("meeting.meetingAgenda").alias("meetingAgenda"),
            F.col("meeting.planningInspectorateRole").alias("planningInspectorateRole"),
            F.col("meeting.meetingDate").alias("meetingDate"),
            F.col("meeting.meetingType").alias("meetingType"),
            F.col("meeting.estimatedPrelimMeetingDate").alias("estimatedPrelimMeetingDate"),
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
        ).where("meetingId is not null")

        # Step 3: Align source schema to target if target exists
        if target_df is not None and not target_df.isEmpty():
            service_bus_exploded = _align_source_to_target_schema(service_bus_exploded, target_df)

        # Step 4: Create business key and row hash
        all_hash_cols = _BUSINESS_KEY_COLS + _CHANGE_TRACKING_COLS
        source_df = service_bus_exploded.withColumn("business_key", F.sha2(F.concat_ws("~", *_BUSINESS_KEY_COLS), 256)).withColumn(
            "row_hash", F.sha2(F.concat_ws("~", *all_hash_cols), 256)
        )

        # Step 5: Deduplicate source - keep latest per business_key by IngestionDate desc
        window_dedup = Window.partitionBy("business_key").orderBy(F.col("IngestionDate").desc())
        source_df_deduped = source_df.withColumn("rn", F.row_number().over(window_dedup)).filter("rn = 1").drop("rn")

        # Step 6: SCD Type 2 logic
        insert_count = 0
        update_count = 0

        if target_df is None or target_df.isEmpty():
            # Initial load
            LoggingUtil().log_info("Target table empty — performing initial load")
            window_spec = Window.orderBy("business_key")
            initial_load = (
                source_df_deduped.withColumn("NSIPMeetingId", F.row_number().over(window_spec))
                .withColumn("ValidTo", F.lit(None).cast("timestamp"))
                .withColumn("IsActive", F.lit("Y"))
                .drop("business_key")
            )
            insert_count = initial_load.count()
            final_df = initial_load
            write_mode = "overwrite"
        else:
            # Incremental SCD2
            LoggingUtil().log_info("Target table has data — performing SCD2 incremental load")

            # Get max NSIPMeetingId
            max_id_result = target_df.agg(F.max("NSIPMeetingId").alias("max_id")).collect()[0]["max_id"]
            max_id = max_id_result if max_id_result is not None else 0

            # Read current active harmonised records
            current_harmonised_active = (
                target_df.filter("IsActive = 'Y'")
                .withColumn("business_key", F.sha2(F.concat_ws("~", *_BUSINESS_KEY_COLS), 256))
                .select("business_key", "row_hash", "NSIPMeetingId")
            )

            # Identify changed records: exist in both but hash differs
            changed_records = (
                source_df_deduped.alias("source")
                .join(
                    current_harmonised_active.alias("target"),
                    F.col("source.business_key") == F.col("target.business_key"),
                    "inner",
                )
                .filter(F.col("source.row_hash") != F.col("target.row_hash"))
                .select(
                    F.col("target.NSIPMeetingId"),
                    F.col("target.business_key"),
                )
                .distinct()
            )

            update_count = changed_records.count()

            # Expire old records if changes detected
            if update_count > 0:
                all_harmonised = target_df.withColumn("business_key", F.sha2(F.concat_ws("~", *_BUSINESS_KEY_COLS), 256))

                expired_df = (
                    all_harmonised.alias("target")
                    .join(
                        changed_records.alias("expire"),
                        (F.col("target.NSIPMeetingId") == F.col("expire.NSIPMeetingId"))
                        & (F.col("target.business_key") == F.col("expire.business_key")),
                        "left",
                    )
                    .withColumn(
                        "IsActive",
                        F.when(F.col("expire.NSIPMeetingId").isNotNull(), F.lit("N")).otherwise(F.col("target.IsActive")),
                    )
                    .withColumn(
                        "ValidTo",
                        F.when(F.col("expire.NSIPMeetingId").isNotNull(), F.current_timestamp()).otherwise(F.col("target.ValidTo")),
                    )
                    .select("target.*", "IsActive", "ValidTo")
                    .drop("business_key")
                )

                # This expired_df must be written as overwrite to replace the target
                expired_df.createOrReplaceTempView("nsip_meeting_expired")

            # Find new records (never seen before)
            new_records = source_df_deduped.alias("source").join(
                current_harmonised_active.select("business_key").distinct().alias("target"),
                F.col("source.business_key") == F.col("target.business_key"),
                "left_anti",
            )

            # Find changed records that need new versions
            changed_records_full = (
                source_df_deduped.alias("source")
                .join(
                    current_harmonised_active.alias("target"),
                    F.col("source.business_key") == F.col("target.business_key"),
                    "inner",
                )
                .filter(F.col("source.row_hash") != F.col("target.row_hash"))
                .select("source.*")
            )

            # Union new and changed
            records_to_insert = new_records.unionByName(changed_records_full)
            insert_count = records_to_insert.count()

            if insert_count > 0:
                window_spec = Window.orderBy("business_key", "row_hash")
                new_versions = (
                    records_to_insert.withColumn("temp_row_num", F.row_number().over(window_spec))
                    .withColumn("NSIPMeetingId", F.col("temp_row_num") + F.lit(max_id))
                    .drop("temp_row_num")
                    .withColumn("ValidTo", F.lit(None).cast("timestamp"))
                    .withColumn("IsActive", F.lit("Y"))
                    .drop("business_key")
                )
                new_versions.createOrReplaceTempView("nsip_meeting_new_versions")

            # Build final output: if we expired records, overwrite the base; then append new
            # Since we can only return one DataFrame per table in data_to_write,
            # and the SCD2 pattern requires overwrite + append, we handle it here:
            if update_count > 0 and insert_count > 0:
                # expired_df (overwrite base) + new_versions (append)
                # Combine: overwrite with expired_df union new_versions
                final_df = expired_df.unionByName(new_versions, allowMissingColumns=True)
                write_mode = "overwrite"
            elif update_count > 0:
                final_df = expired_df
                write_mode = "overwrite"
            elif insert_count > 0:
                final_df = new_versions
                write_mode = "append"
            else:
                LoggingUtil().log_info("No new or changed records to process")
                final_df = self.spark.createDataFrame([], target_df.schema)
                write_mode = "append"

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "sb_nsip_meeting",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "sb_nsip_meeting",
                "file_format": "delta",
                "write_mode": write_mode,
                "write_options": {"overwriteSchema": "true"} if write_mode == "overwrite" else {},
            }
        }

        end_exec_time = datetime.now()
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
