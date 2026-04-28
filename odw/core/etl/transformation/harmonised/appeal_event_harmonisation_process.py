from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict


class AppealEventHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "appeal_event"
    PRIMARY_KEY = "eventId"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.spark = spark
        self.harmonised_db = "odw_harmonised_db"
        self.standardised_db = "odw_standardised_db"

    @classmethod
    def get_name(cls) -> str:
        return "appeal_event_harmonisation_process"

    def load_data(self, data_to_read=None, **kwargs) -> Dict[str, DataFrame]:
        service_bus_table = f"{self.harmonised_db}.sb_appeal_event"
        horizon_table = f"{self.standardised_db}.horizon_appeals_event"

        service_bus_df = self.spark.sql(f"""
            SELECT DISTINCT
                AppealsEventId,
                eventId,
                caseReference,
                eventType,
                eventName,
                eventstatus AS eventStatus,
                isUrgent,
                eventPublished,
                eventStartDatetime,
                eventEndDatetime,
                notificationOfSitevisit,
                AddressLine1 AS addressLine1,
                AddressLine2 AS addressLine2,
                AddressTown AS addressTown,
                AddressCounty AS addressCounty,
                AddressPostcode AS addressPostcode,
                Migrated,
                ODTSourceSystem,
                SourceSystemID,
                IngestionDate,
                ValidTo,
                RowID,
                IsActive
            FROM {service_bus_table}
        """)

        horizon_df = self.spark.sql(f"""
            SELECT DISTINCT
                CAST(NULL AS LONG) AS AppealsEventId,
                CONCAT(casenumber, '-', eventId) AS eventId,
                caseReference,
                eventType,
                eventName,
                eventStatus,
                CAST(NULL AS BOOLEAN) AS isUrgent,
                CAST(NULL AS BOOLEAN) AS eventPublished,
                CAST(NULL AS STRING) AS eventStartDatetime,
                CAST(NULL AS STRING) AS eventEndDatetime,
                notificationOfSitevisit,
                eventaddressline1 AS addressLine1,
                eventaddressline2 AS addressLine2,
                eventaddresstown AS addressTown,
                eventaddresscounty AS addressCounty,
                eventaddresspostcode AS addressPostcode,
                0 AS Migrated,
                'Horizon' AS ODTSourceSystem,
                9 AS SourceSystemID,
                ingested_datetime AS IngestionDate,
                CAST(NULL AS STRING) AS ValidTo,
                '' AS RowID,
                'Y' AS IsActive
            FROM {horizon_table}
            WHERE ingested_datetime = (
                SELECT MAX(ingested_datetime)
                FROM {horizon_table}
            )
        """)

        horizon_df = horizon_df.select(service_bus_df.columns)

        return {
            "service_bus": service_bus_df,
            "horizon": horizon_df,
        }

    def process(self, source_data: Dict[str, DataFrame], **kwargs):
        start_exec_time = datetime.now()

        combined_df = source_data["service_bus"].union(source_data["horizon"])

        target_table = f"{self.harmonised_db}.{self.OUTPUT_TABLE}"

        combined_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

        combined_df = self.spark.table(target_table)
        combined_df.createOrReplaceTempView("appeal_event_base")

        self.spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW vw_appeal_event_calculations_base AS
            SELECT
                row_number() OVER (
                    PARTITION BY {self.PRIMARY_KEY}
                    ORDER BY IngestionDate DESC
                ) AS ReverseOrderProcessed,
                row_number() OVER (
                    ORDER BY IngestionDate ASC, {self.PRIMARY_KEY} ASC
                ) AS AppealsEventId,
                {self.PRIMARY_KEY},
                IngestionDate,
                ValidTo,
                '0' AS Migrated,
                CASE
                    WHEN row_number() OVER (
                        PARTITION BY {self.PRIMARY_KEY}
                        ORDER BY IngestionDate DESC
                    ) = 1 THEN 'Y'
                    ELSE 'N'
                END AS IsActive
            FROM appeal_event_base
        """)

        calcs_df = self.spark.sql(f"""
            SELECT
                Current.AppealsEventId,
                Current.{self.PRIMARY_KEY} AS eventId,
                Current.IngestionDate,
                NextRow.IngestionDate AS ValidTo,
                CASE
                    WHEN Raw.{self.PRIMARY_KEY} IS NOT NULL THEN '1'
                    ELSE '0'
                END AS Migrated,
                Current.IsActive
            FROM vw_appeal_event_calculations_base Current
            LEFT JOIN vw_appeal_event_calculations_base NextRow
                ON Current.{self.PRIMARY_KEY} = NextRow.{self.PRIMARY_KEY}
               AND Current.ReverseOrderProcessed - 1 = NextRow.ReverseOrderProcessed
            LEFT JOIN (
                SELECT DISTINCT eventId
                FROM odw_harmonised_db.sb_appeal_event
            ) Raw
                ON Current.{self.PRIMARY_KEY} = Raw.{self.PRIMARY_KEY}
        """)

        final_columns = combined_df.columns

        final_df = (
            combined_df.drop("AppealsEventId", "ValidTo", "Migrated", "IsActive")
            .join(calcs_df, on=["eventId", "IngestionDate"], how="inner")
            .select(final_columns)
            .distinct()
        )

        final_df = final_df.withColumn("RowID", F.md5(F.concat_ws("", *[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in final_df.columns])))

        hrm_table_snake_case = self.OUTPUT_TABLE.replace("-", "_")

        data_to_write = {
            target_table: {
                "data": final_df,
                "storage_kind": "ADLSG2-LegacyDelta",
                "database_name": self.harmonised_db,
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": hrm_table_snake_case,
                "merge_keys": [self.PRIMARY_KEY],
            }
        }

        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=datetime.now(),
                table_name=target_table,
                insert_count=final_df.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.get_name(),
                duration_seconds=(datetime.now() - start_exec_time).total_seconds(),
            )
        )
