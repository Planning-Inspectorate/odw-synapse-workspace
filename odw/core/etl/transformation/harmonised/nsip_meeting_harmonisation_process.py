from odw.core.etl.transformation.harmonised.harmonisation_process import (
    HarmonisationProcess,
)
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Business key columns for SCD2.
_BUSINESS_KEY_COLS = ["caseId", "meetingId"]

# Change tracking columns (combined with business key for row_hash)
_CHANGE_TRACKING_COLS = [
    "meetingAgenda",
    "planningInspectorateRole",
    "meetingType",
    "meetingDate",
]


def _null_safe_concat_expr(columns) -> F.Column:
    return F.concat_ws(
        "~", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    )


def _align_source_to_target_schema(
    source_df: DataFrame, target_df: DataFrame
) -> DataFrame:
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
            aligned_columns.append(
                F.col(col_name).cast(target_schema[col_name]).alias(col_name)
            )
        else:
            aligned_columns.append(F.col(col_name))
    return source_df.select(aligned_columns)


class NsipMeetingHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Meeting data using SCD Type 2 logic.

    # Example usage via py_etl_executor

    ```
    input_arguments = {
        "etl_process_name": "NSIP Meeting Harmonisation Process",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_project"
    OUTPUT_TABLE = "sb_nsip_meeting"

    @classmethod
    def get_name(cls) -> str:
        return "NSIP Meeting Harmonisation Process"

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
        LoggingUtil().log_info(
            f"Loading target table odw_harmonised_db.{self.OUTPUT_TABLE}"
        )
        if self.spark.catalog.tableExists(f"odw_harmonised_db.{self.OUTPUT_TABLE}"):
            target_df = self.spark.table(f"odw_harmonised_db.{self.OUTPUT_TABLE}")
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
        service_bus_data: DataFrame = self.load_parameter(
            "service_bus_data", source_data
        )
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
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
        ).where("meetingId is not null")

        # Step 3: Align source schema to target if target exists
        if target_df is not None and not target_df.isEmpty():
            service_bus_exploded = _align_source_to_target_schema(
                service_bus_exploded, target_df
            )

        # Step 4: Create business key and row hash
        all_hash_cols = _BUSINESS_KEY_COLS + _CHANGE_TRACKING_COLS
        source_df = service_bus_exploded.withColumns(
            {
                "business_key": F.sha2(_null_safe_concat_expr(_BUSINESS_KEY_COLS), 256),
                "row_hash": F.sha2(_null_safe_concat_expr(all_hash_cols), 256),
            }
        )

        # Step 5: Rank all source rows per business_key so that we never lose any source version.
        # Latest version per key -> key_rank_desc == 1. For historical rows, next_msg_ingestion_date
        # (obtained via lead in ascending order) becomes the ValidTo timestamp.
        source_rank_window_desc = Window.partitionBy("business_key").orderBy(
            F.col("IngestionDate").desc(),
            F.col("NSIPProjectInfoInternalID").desc(),
            F.col("row_hash").desc(),
        )
        source_rank_window_asc = Window.partitionBy("business_key").orderBy(
            F.col("IngestionDate").asc(),
            F.col("NSIPProjectInfoInternalID").asc(),
            F.col("row_hash").asc(),
        )
        source_df_ranked = source_df.withColumns(
            {
                "key_rank_desc": F.row_number().over(source_rank_window_desc),
                "next_msg_ingestion_date": F.lead("IngestionDate").over(
                    source_rank_window_asc
                ),
            }
        )
        latest_source_per_key = source_df_ranked.filter(F.col("key_rank_desc") == 1)

        # Step 6: SCD Type 2 logic
        insert_count = 0
        update_count = 0

        if target_df is None or target_df.isEmpty():
            # Initial load: keep every source version. Latest per key -> IsActive=Y, ValidTo=NULL.
            # Older versions -> IsActive=N, ValidTo=next_msg_ingestion_date.
            LoggingUtil().log_info("Target table empty — performing initial load")
            window_spec = Window.orderBy(
                "business_key", "IngestionDate", "NSIPProjectInfoInternalID", "row_hash"
            )
            initial_load = source_df_ranked.withColumns(
                {
                    "NSIPMeetingId": F.row_number().over(window_spec),
                    "IsActive": F.when(
                        F.col("key_rank_desc") == 1, F.lit("Y")
                    ).otherwise(F.lit("N")),
                    "ValidTo": F.when(
                        F.col("key_rank_desc") == 1, F.lit(None).cast("timestamp")
                    ).otherwise(F.col("next_msg_ingestion_date").cast("timestamp")),
                }
            ).drop("business_key", "key_rank_desc", "next_msg_ingestion_date")
            insert_count = initial_load.count()
            final_df = initial_load
            write_mode = "overwrite"
        else:
            LoggingUtil().log_info(
                "Target table has data — performing SCD2 incremental load"
            )

            # Get max NSIPMeetingId
            max_id_result = target_df.agg(
                F.max("NSIPMeetingId").alias("max_id")
            ).collect()[0]["max_id"]
            max_id = max_id_result if max_id_result is not None else 0

            # Active harmonised records
            current_harmonised_active = (
                target_df.filter("IsActive = 'Y'")
                .withColumn(
                    "business_key",
                    F.sha2(_null_safe_concat_expr(_BUSINESS_KEY_COLS), 256),
                )
                .select("business_key", "row_hash", "NSIPMeetingId")
            )

            # Changed keys: latest source differs from currently active target
            changed_keys = (
                latest_source_per_key.alias("source")
                .join(
                    current_harmonised_active.alias("target"),
                    F.col("source.business_key") == F.col("target.business_key"),
                    "inner",
                )
                .filter(F.col("source.row_hash") != F.col("target.row_hash"))
                .select(F.col("target.business_key").alias("business_key"))
                .distinct()
            )

            # New keys: not present in active target
            new_keys = (
                latest_source_per_key.alias("source")
                .join(
                    current_harmonised_active.select("business_key")
                    .distinct()
                    .alias("target"),
                    F.col("source.business_key") == F.col("target.business_key"),
                    "left_anti",
                )
                .select(F.col("source.business_key").alias("business_key"))
                .distinct()
            )

            keys_to_process = changed_keys.unionByName(new_keys).distinct()
            keys_to_process_count = keys_to_process.count()
            update_count = changed_keys.count()

            expired_df = None
            new_versions = None

            if keys_to_process_count > 0:
                # For expiring currently active target rows, use first incoming message
                # datetime for that key.
                first_incoming_per_key = (
                    source_df_ranked.alias("s")
                    .join(
                        keys_to_process.alias("k"),
                        F.col("s.business_key") == F.col("k.business_key"),
                        "inner",
                    )
                    .groupBy(F.col("s.business_key").alias("business_key"))
                    .agg(
                        F.min(F.col("s.IngestionDate")).alias(
                            "first_new_msg_ingestion_date"
                        )
                    )
                )

                all_harmonised = target_df.withColumn(
                    "business_key",
                    F.sha2(_null_safe_concat_expr(_BUSINESS_KEY_COLS), 256),
                )

                target_cols_to_keep = [
                    c
                    for c in all_harmonised.columns
                    if c not in ("IsActive", "ValidTo", "business_key")
                ]

                expired_df = (
                    all_harmonised.alias("target")
                    .join(
                        first_incoming_per_key.alias("expire"),
                        F.col("target.business_key") == F.col("expire.business_key"),
                        "left",
                    )
                    .select(
                        *[F.col(f"target.{c}") for c in target_cols_to_keep],
                        F.when(
                            (F.col("expire.business_key").isNotNull())
                            & (F.col("target.IsActive") == F.lit("Y")),
                            F.lit("N"),
                        )
                        .otherwise(F.col("target.IsActive"))
                        .alias("IsActive"),
                        F.when(
                            (F.col("expire.business_key").isNotNull())
                            & (F.col("target.IsActive") == F.lit("Y")),
                            F.col("expire.first_new_msg_ingestion_date").cast(
                                "timestamp"
                            ),
                        )
                        .otherwise(F.col("target.ValidTo"))
                        .alias("ValidTo"),
                    )
                )

                # Insert all source rows for affected keys (new or changed)
                records_to_insert = (
                    source_df_ranked.alias("source")
                    .join(
                        keys_to_process.alias("keys"),
                        F.col("source.business_key") == F.col("keys.business_key"),
                        "inner",
                    )
                    .select("source.*")
                )

                insert_count = records_to_insert.count()

                if insert_count > 0:
                    window_spec = Window.orderBy(
                        "business_key",
                        "IngestionDate",
                        "NSIPProjectInfoInternalID",
                        "row_hash",
                    )
                    new_versions = records_to_insert.withColumns(
                        {
                            "NSIPMeetingId": F.row_number().over(window_spec)
                            + F.lit(max_id),
                            "IsActive": F.when(
                                F.col("key_rank_desc") == 1, F.lit("Y")
                            ).otherwise(F.lit("N")),
                            "ValidTo": F.when(
                                F.col("key_rank_desc") == 1,
                                F.lit(None).cast("timestamp"),
                            ).otherwise(
                                F.col("next_msg_ingestion_date").cast("timestamp")
                            ),
                        }
                    ).drop("business_key", "key_rank_desc", "next_msg_ingestion_date")

            # Build final output. Since data_to_write supports a single DataFrame per table
            # and the SCD2 pattern requires overwrite (expired) + append (new), we combine
            # expired_df and new_versions and write via overwrite (matches notebook behavior).
            if expired_df is not None and new_versions is not None:
                final_df = expired_df.unionByName(
                    new_versions, allowMissingColumns=True
                )
                write_mode = "overwrite"
            elif expired_df is not None:
                final_df = expired_df
                write_mode = "overwrite"
            elif new_versions is not None:
                final_df = new_versions
                write_mode = "append"
            else:
                LoggingUtil().log_info("No new or changed records to process")
                final_df = self.spark.createDataFrame([], target_df.schema)
                write_mode = "append"

        data_to_write = {
            f"odw_harmonised_db.{self.OUTPUT_TABLE}": {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": self.OUTPUT_TABLE,
                "file_format": "delta",
                "write_mode": write_mode,
                "write_options": {"overwriteSchema": "true"}
                if write_mode == "overwrite"
                else {},
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=f"odw_harmonised_db.{self.OUTPUT_TABLE}",
                insert_count=insert_count,
                update_count=update_count,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
