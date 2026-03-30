from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Columns used to build the MD5 RowID hash, matching the IFNULL(CAST(... AS String), '.') list
_S51_ADVICE_ROW_ID_COLUMNS = [
    "NSIPAdviceID",
    "adviceId",
    "adviceReference",
    "caseId",
    "caseReference",
    "title",
    "titleWelsh",
    "from",
    "agent",
    "method",
    "enquiryDate",
    "enquiryDetails",
    "enquiryDetailsWelsh",
    "adviceGivenBy",
    "adviceDate",
    "adviceDetails",
    "adviceDetailsWelsh",
    "status",
    "redactionStatus",
    "attachmentIds",
    # Horizon-only fields
    "Section51Advice",
    "EnquirerFirstName",
    "EnquirerLastName",
    "AdviceLastModified",
    "AttachmentCount",
    "AttachmentsLastModified",
    "LastPublishedDate",
    "WelshLanguage",
    "CaseWorkType",
    # System fields included in RowID
    "Migrated",
    "ODTSourceSystem",
    "IngestionDate",
    "ValidTo",
]


class NsipS51AdviceHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP S51 Advice data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-s51-advice-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_s51_advice"
    HORIZON_TABLE = "odw_standardised_db.horizon_nsip_advice"
    OUTPUT_TABLE = "odw_harmonised_db.nsip_s51_advice"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-s51-advice-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE}")
        horizon_data = self._load_horizon_data()

        LoggingUtil().log_info("Identifying deleted Horizon records")
        horizon_deleted = self._load_horizon_deleted()

        sb_advice_ids = self._load_service_bus_advice_ids()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
            "horizon_deleted": horizon_deleted,
            "sb_advice_ids": sb_advice_ids,
        }

    def _load_service_bus_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                NSIPAdviceID
                ,adviceId
                ,adviceReference
                ,caseId
                ,caseReference
                ,title
                ,titleWelsh
                ,`from`
                ,agent
                ,method
                ,enquiryDate
                ,enquiryDetails
                ,enquiryDetailsWelsh
                ,adviceGivenBy
                ,adviceDate
                ,adviceDetails
                ,adviceDetailsWelsh
                ,status
                ,redactionStatus
                ,attachmentIds

                ,'Yes' as Section51Advice
                ,CAST(NULL AS String) as EnquirerFirstName
                ,CAST(NULL AS String) as EnquirerLastName
                ,CAST(NULL AS String) as AdviceLastModified
                ,CAST(NULL AS String) as AttachmentCount
                ,CAST(NULL AS String) as AttachmentsLastModified
                ,CAST(NULL AS String) as LastPublishedDate
                ,CAST(NULL AS String) as WelshLanguage
                ,CAST(NULL AS String) as CaseWorkType
                ,CAST(NULL AS String) as AttachmentModifyDate

                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
                ,NULLIF(ValidTo, '') AS ValidTo
                ,'' as RowID
                ,IsActive
            FROM
                {self.SERVICE_BUS_TABLE}
        """)

    def _load_horizon_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                CAST(NULL AS Long) as NSIPAdviceID
                ,Cast(advicenodeid as integer) as adviceId
                ,adviceReference
                ,Cast(casenodeid as integer) as caseId
                ,caseReference
                ,title
                ,CAST(NULL AS String) as titleWelsh
                ,concat(enquirerfirstname,' ',enquirerlastname) as `from`
                ,enquirerOrganisation as agent
                ,enqirymethod as method
                ,enquiryDate
                ,enquiry as enquiryDetails
                ,CAST(NULL AS String) as enquiryDetailsWelsh
                ,adviceFrom as adviceGivenBy
                ,adviceDate
                ,advice as adviceDetails
                ,CAST(NULL AS String) as adviceDetailsWelsh
                ,adviceStatus as status
                ,null as redactionStatus
                ,attachmentdataID as attachmentIds

                ,Section51Advice
                ,EnquirerFirstName
                ,EnquirerLastName
                ,AdviceLastModified
                ,AttachmentCount
                ,AttachmentsLastModified
                ,LastPublishedDate
                ,WelshLanguage
                ,CaseWorkType
                ,AttachmentModifyDate

                ,"0" as Migrated
                ,"Horizon" as ODTSourceSystem
                ,NULL AS SourceSystemID
                ,to_timestamp(expected_from) AS IngestionDate
                ,CAST(null as string) as ValidTo
                ,'' as RowID
                ,'Y' as IsActive
            FROM
                {self.HORIZON_TABLE} AS Horizon
            WHERE
                ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_TABLE})
        """)

    def _load_horizon_deleted(self) -> DataFrame:
        """
        Identify Horizon records that have been deleted: present in older snapshots
        but absent from the latest snapshot. Returns distinct advicenodeid values.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT h_all.advicenodeid
            FROM {self.HORIZON_TABLE} h_all
            LEFT ANTI JOIN (
                SELECT * FROM {self.HORIZON_TABLE}
                WHERE ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_TABLE})
            ) h_latest
            ON h_all.advicenodeid = h_latest.advicenodeid
        """)

    def _load_service_bus_advice_ids(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT adviceId
            FROM {self.SERVICE_BUS_TABLE}
        """)

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)
        horizon_deleted: DataFrame = self.load_parameter("horizon_deleted", source_data)
        sb_advice_ids: DataFrame = self.load_parameter("sb_advice_ids", source_data)

        # Step 1: Aggregate Horizon attachmentIds
        LoggingUtil().log_info("Aggregating Horizon attachmentIds")
        horizon_agg = (
            horizon_data.groupBy("adviceId")
            .agg(F.array_sort(F.array_distinct(F.collect_list(F.col("attachmentIds")))).alias("attachmentIds"))
            .orderBy("adviceId")
        )
        horizon_data = horizon_data.drop("attachmentIds")
        horizon_data = horizon_data.join(horizon_agg, on="adviceId", how="inner")

        # Step 2: Align Horizon columns to SB columns and union
        LoggingUtil().log_info(f"Combining data for {self.OUTPUT_TABLE}")
        horizon_data = horizon_data.select(service_bus_data.columns)
        combined = service_bus_data.union(horizon_data)

        # Step 3: Window-function calculations
        win_per_advice_desc = Window.partitionBy("adviceId").orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col("adviceId").asc())

        combined = (
            combined.withColumn("ReverseOrderProcessed", F.row_number().over(win_per_advice_desc))
            .withColumn("NSIPAdviceID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_per_advice_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        # Step 4: Compute ValidTo via self-join
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col("CurrentRow.adviceId") == F.col("NextRow.adviceId"))
            & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.NSIPAdviceID").alias("NSIPAdviceID"),
            F.col("CurrentRow.adviceId").alias("adviceId"),
            F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.col("CurrentRow.IsActive").alias("IsActive"),
        )

        # Step 5: Derive Migrated flag
        sb_ids = sb_advice_ids.withColumnRenamed("adviceId", "sb_adviceId")
        calcs = (
            calcs.join(sb_ids, calcs["adviceId"] == sb_ids["sb_adviceId"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_adviceId").isNotNull(), F.lit("1")).otherwise(F.lit("0")))
            .drop("sb_adviceId")
        )

        # Step 6: Compute RowID via MD5 hash
        # Use backticks for `from` which is a reserved word — access via col("`from`")
        row_id_cols = []
        for c in _S51_ADVICE_ROW_ID_COLUMNS:
            col_ref = F.col(f"`{c}`") if c == "from" else F.col(c)
            row_id_cols.append(F.coalesce(col_ref.cast("string"), F.lit(".")))
        row_id_expr = F.md5(F.concat(*row_id_cols))

        # Step 7: Rejoin calculations back onto the combined dataset
        # The notebook does: SELECT DISTINCT ... FROM spark_table_final (which is the intermediate write)
        # then drops NSIPAdviceID, ValidTo, Migrated, IsActive, joins calcs back, selects columns
        all_columns = [
            c for c in combined.columns
            if c not in {"ReverseOrderProcessed", "SourceSystemID", "AttachmentModifyDate"}
        ]
        columns = all_columns
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("NSIPAdviceID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col("adviceId").alias("calc_adviceId"),
            F.col("IngestionDate").alias("calc_IngestionDate"),
            F.col("NSIPAdviceID"),
            F.col("ValidTo"),
            F.col("Migrated"),
            F.col("IsActive"),
        )

        joined = base.join(
            calcs_renamed,
            (base["adviceId"] == calcs_renamed["calc_adviceId"]) & (base["IngestionDate"] == calcs_renamed["calc_IngestionDate"]),
        ).select(columns)

        # Apply RowID
        final_df = joined.withColumn("RowID", row_id_expr)
        final_df = final_df.dropDuplicates()

        # Step 8: Apply Horizon delete logic
        deleted_ids = [row[0] for row in horizon_deleted.collect()]

        # Set IsActive to N if the adviceId has been deleted from Horizon
        hrm_updated = final_df.withColumn("IsActive", F.when(F.col("adviceId").isin(deleted_ids), F.lit("N")).otherwise(F.col("IsActive")))

        # Set ValidTo to null if IsActive = Y
        hrm_updated = hrm_updated.withColumn("ValidTo", F.when(F.col("IsActive") == "Y", F.lit(None)).otherwise(F.col("ValidTo")))

        # Set ValidTo if IsActive = N and ValidTo is null to IngestionDate + 1 day
        hrm_final = hrm_updated.withColumn(
            "ValidTo",
            F.when(
                (F.col("ValidTo").isNull()) & (F.col("IsActive") == "N"),
                F.date_add(F.col("IngestionDate"), 1).cast("timestamp"),
            ).otherwise(F.col("ValidTo")),
        )

        insert_count = hrm_final.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": hrm_final,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "nsip_s51_advice",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "nsip_s51_advice",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["IsActive"],
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
