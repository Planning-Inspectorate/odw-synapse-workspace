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
# in the original notebook's final SELECT DISTINCT
_APPEAL_DOCUMENT_ROW_ID_COLUMNS = [
    "AppealsDocumentMetadataID",
    "documentId",
    "caseId",
    "caseReference",
    "version",
    "filename",
    "originalFilename",
    "size",
    "mime",
    "documentURI",
    "publishedDocumentURI",
    "virusCheckStatus",
    "fileMD5",
    "dateCreated",
    "dateReceived",
    "datePublished",
    "lastModified",
    "caseType",
    "redactedStatus",
    "documentType",
    "sourceSystem",
    "origin",
    "owner",
    "author",
    "description",
    "caseStage",
    "horizonFolderId",
    # Horizon-only fields
    "caseNumber",
    "caseworkTypeGroup",
    "caseworkTypeAbbreviation",
    "versionFilename",
    "incomingOutgoingExternal",
    "publishedStatus",
]


class AppealDocumentHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising Appeal Document data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal-document-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_appeal_document"
    HORIZON_TABLE = "odw_standardised_db.horizon_appeals_document_metadata"
    AIE_EXTRACTS_TABLE = "odw_harmonised_db.aie_document_data"
    OUTPUT_TABLE = "odw_harmonised_db.appeal_document"
    PRIMARY_KEY = "TEMP_PK"
    
    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal-document-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load data from:
        - Service bus harmonised table (sb_appeal_document)
        - Horizon standardised table (horizon_appeals_document_metadata)
        - AIE extracts table (aie_document_data)
        No joins or transformations are applied here – only reads.
        """
        try:
            self.spark.catalog.refreshTable(self.HORIZON_TABLE)
        except Exception:
            LoggingUtil().log_info(f"Could not refresh table {self.HORIZON_TABLE}, continuing")

        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE}")
        horizon_data = self._load_horizon_data()

        LoggingUtil().log_info(f"Loading AIE extracts data from {self.AIE_EXTRACTS_TABLE}")
        aie_data = self._load_aie_data()

        # Also load the set of primary keys that exist in the service-bus table.
        # This is used later to derive the Migrated flag.
        sb_primary_keys = self._load_service_bus_primary_keys()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
            "aie_data": aie_data,
            "sb_primary_keys": sb_primary_keys,
        }

    def _load_service_bus_data(self) -> DataFrame:
        """
        Get data out of the service bus with additional fields needed for Horizon data.
        Computes a primary key: MD5(CONCAT(documentId, filename, version, documentURI))
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(documentId, filename, version, documentURI)) AS {self.PRIMARY_KEY}
                ,AppealsDocumentMetadataID
                ,documentId
                ,caseId
                ,caseReference
                ,version
                ,filename
                ,originalFilename
                ,size
                ,mime
                ,documentURI
                ,publishedDocumentURI
                ,virusCheckStatus
                ,fileMD5
                ,dateCreated
                ,dateReceived
                ,datePublished
                ,lastModified
                ,caseType
                ,redactedStatus
                ,documentType
                ,sourceSystem
                ,origin
                ,owner
                ,author
                ,description
                ,caseStage
                ,horizonFolderId

                ,CAST(NULL AS String) AS caseNumber
                ,CAST(NULL AS String) AS caseworkTypeGroup
                ,CAST(NULL AS String) AS caseworkTypeAbbreviation
                ,CAST(NULL AS String) AS versionFilename
                ,CAST(NULL AS String) AS incomingOutgoingExternal
                ,CAST(NULL AS String) AS publishedStatus
                
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
        """
        Read Horizon document metadata, filtered to the latest ingested_datetime date.
        No joins are applied here – the AIE join happens in process().
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                documentId
                ,casenodeid
                ,caseReference
                ,version
                ,filename
                ,size
                ,virusCheckStatus
                ,dateCreated
                ,datePublished
                ,lastModified
                ,caseworkType
                ,redactedStatus
                ,documentType
                ,sourceSystem
                ,documentDescription
                ,folderid
                ,caseNumber
                ,caseworkTypeGroup
                ,caseworkTypeAbbreviation
                ,versionFilename
                ,incomingOutgoingExternal
                ,publishedStatus
                ,expected_from
            FROM
                {self.HORIZON_TABLE}
            WHERE
                ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_TABLE})
        """)

    def _load_aie_data(self) -> DataFrame:
        """
        Read AIE document extract data.
        This is joined to Horizon data in process().
        """
        return self.spark.sql(f"""
            SELECT
                documentid
                ,version
                ,size
                ,mime
                ,documentURI
                ,fileMD5
                ,owner
                ,author
            FROM
                {self.AIE_EXTRACTS_TABLE}
        """)
    
    def _load_service_bus_primary_keys(self) -> DataFrame:
        """
        Load the distinct set of primary keys from the service-bus table.
        Used to derive the Migrated flag (1 if the PK exists in SB, else 0).
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(documentId, filename, version, documentURI)) AS {self.PRIMARY_KEY}
            FROM {self.SERVICE_BUS_TABLE}
        """)
    
    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Combine service bus and Horizon data, compute IsActive/ValidTo/Migrated/RowID,
        deduplicate, and return the data_to_write dictionary for the base write_data step.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)
        aie_data: DataFrame = self.load_parameter("aie_data", source_data)
        sb_primary_keys: DataFrame = self.load_parameter("sb_primary_keys", source_data)

        # ---------------------------------------------------------
        # Step 1: Join Horizon + AIE
        # ---------------------------------------------------------
        LoggingUtil().log_info("Joining Horizon with AIE data and aligning to SB schema")

        horizon_joined = (
            horizon_data.alias("Doc")
            .join(
                aie_data.alias("Aie"),
                (F.col("Doc.documentId") == F.col("Aie.documentid")) &
                (F.col("Doc.size") == F.col("Aie.size")) &
                (F.col("Doc.version") == F.col("Aie.version")),
                "left",
            )
            .select(
                F.md5(
                    F.concat(
                        F.coalesce(F.col("Doc.documentId"), F.lit("")),
                        F.coalesce(F.col("Doc.filename"), F.lit("")),
                        F.coalesce(F.col("Doc.version").cast("integer").cast("string"), F.lit("")),
                        F.coalesce(F.col("Aie.documentURI"), F.lit("")),
                    )
                ).alias(self.PRIMARY_KEY),
                F.lit(None).cast("long").alias("AppealsDocumentMetadataID"),
                F.col("Doc.documentId").alias("documentId"),
                F.col("Doc.casenodeid").cast("integer").alias("caseId"),
                F.col("Doc.caseReference"),
                F.col("Doc.version").cast("integer").alias("version"),
                F.col("Doc.filename").alias("filename"),
                F.col("Doc.filename").alias("originalFilename"),
                F.col("Doc.size").cast("integer").alias("size"),
                F.col("Aie.mime"),
                F.col("Aie.documentURI"),
                F.lit(None).cast("string").alias("publishedDocumentURI"),
                F.col("Doc.virusCheckStatus"),
                F.col("Aie.fileMD5"),
                F.col("Doc.dateCreated").alias("dateCreated"),
                F.lit(None).cast("string").alias("dateReceived"),
                F.col("Doc.datePublished"),
                F.col("Doc.lastModified"),
                F.col("Doc.caseworkType").alias("caseType"),
                F.col("Doc.redactedStatus"),
                F.col("Doc.documentType"),
                F.col("Doc.sourceSystem"),
                F.lit(None).cast("string").alias("origin"),
                F.col("Aie.owner"),
                F.col("Aie.author"),
                F.col("Doc.documentDescription").alias("description"),
                F.lit(None).cast("string").alias("caseStage"),
                F.col("Doc.folderid").alias("horizonFolderId"),
                F.col("Doc.caseNumber"),
                F.col("Doc.caseworkTypeGroup"),
                F.col("Doc.caseworkTypeAbbreviation"),
                F.col("Doc.versionFilename"),
                F.col("Doc.incomingOutgoingExternal"),
                F.col("Doc.publishedStatus"),
                F.lit("0").alias("Migrated"),
                F.lit("Horizon").alias("ODTSourceSystem"),
                F.lit(None).cast("string").alias("SourceSystemID"),
                F.to_timestamp(F.col("Doc.expected_from")).alias("IngestionDate"),
                F.lit(None).cast("string").alias("ValidTo"),
                F.lit("").alias("RowID"),
                F.lit("Y").alias("IsActive"),
            )
            .distinct()
        )

        # ---------------------------------------------------------
        # Step 2: Align Horizon columns to SB columns and union
        # ---------------------------------------------------------
        LoggingUtil().log_info(f"Combining data for {self.OUTPUT_TABLE}")
        horizon_joined = horizon_joined.select(service_bus_data.columns)
        combined = service_bus_data.union(horizon_joined)

        # ---------------------------------------------------------
        # Step 3: Compute RowID from the combined (pre-correction) data.
        # ---------------------------------------------------------
        row_id_expr = F.md5(
            F.concat(*[
                F.coalesce(F.col(c).cast("string"), F.lit("."))
                for c in _APPEAL_DOCUMENT_ROW_ID_COLUMNS
            ])
        )
        combined = combined.withColumn("RowID", row_id_expr)

        # ---------------------------------------------------------
        # Step 4: Window-function calculations (replaces intermediate table + SQL views)
        # ---------------------------------------------------------
        pk = self.PRIMARY_KEY
        win_pk_desc = Window.partitionBy(pk).orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col(pk).asc())

        combined = (
            combined.withColumn("ReverseOrderProcessed", F.row_number().over(win_pk_desc))
            .withColumn("AppealsDocumentMetadataID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_pk_desc) == 1, "Y").otherwise("N"),
            )
        )

        # ---------------------------------------------------------
        # Step 5: Compute ValidTo by self-joining on (PK, ReverseOrderProcessed - 1)
        # ---------------------------------------------------------
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col(f"CurrentRow.{pk}") == F.col(f"NextRow.{pk}")) &
            (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.AppealsDocumentMetadataID"),
            F.col(f"CurrentRow.{pk}").alias(pk),
            F.col("CurrentRow.IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", None).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.col("CurrentRow.IsActive"),
        )

        # ---------------------------------------------------------
        # Step 6: Derive Migrated flag (1 if PK exists in SB table, else 0)
        # ---------------------------------------------------------
        sb_keys = sb_primary_keys.withColumnRenamed(pk, "sb_pk")
        calcs = (
            calcs.join(sb_keys, calcs[pk] == sb_keys["sb_pk"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_pk").isNotNull(), "1").otherwise("0"))
            .drop("sb_pk")
        )

        # ---------------------------------------------------------
        # Step 7: Rejoin calculations back onto the combined dataset.
        # Mirror the notebook: take SELECT DISTINCT of all columns from the
        # intermediate data, drop the four columns that calcs will replace,
        # then join calcs back and .select(columns) to restore the full column set.
        # ---------------------------------------------------------
        
        all_columns = [c for c in combined.columns if c not in {"ReverseOrderProcessed", "SourceSystemID"}]
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("AppealsDocumentMetadataID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col(pk).alias(f"calc_{pk}"),
            F.col("IngestionDate").alias("calc_IngestionDate"),
            "AppealsDocumentMetadataID",
            "ValidTo",
            "Migrated",
            "IsActive",
        )

        final_df = base.join(
            calcs_renamed,
            (base[pk] == calcs_renamed[f"calc_{pk}"]) &
            (base["IngestionDate"] == calcs_renamed["calc_IngestionDate"]),
        ).select(all_columns).drop(pk).dropDuplicates()

        # Ensure final column order: put SourceSystemID where it belongs if present
        # (The base run() -> write_data() will handle the actual write)
        
        insert_count = final_df.count()

        # ---------------------------------------------------------
        # Step 8: Resolve table path
        # ---------------------------------------------------------
        df = self.spark.sql(f"DESCRIBE EXTENDED {self.OUTPUT_TABLE}")
        rows = df.filter(df.col_name == "Location").select("data_type").collect()
        table_path = rows[0]["data_type"] if rows else None

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "appeal_document",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "appeal_document",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["IsActive"],
                "path": table_path,
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
    