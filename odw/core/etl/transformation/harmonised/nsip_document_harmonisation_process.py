from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Columns used to build the MD5 RowID hash, in the order defined by the original notebook
_ROW_ID_COLUMNS = [
    "NSIPDocumentID", "documentId", "caseId", "caseRef", "version",
    "examinationRefNo", "filename", "originalFilename", "size", "mime",
    "documentURI", "publishedDocumentURI", "path", "virusCheckStatus",
    "fileMD5", "dateCreated", "datePublished", "lastModified", "caseType",
    "redactedStatus", "documentType", "securityClassification", "sourceSystem",
    "origin", "owner", "author", "authorWelsh", "description",
    "descriptionWelsh", "documentCaseStage", "filter1", "filter1Welsh",
    "filter2", "horizonFolderId", "transcriptId", "publishedStatus",
    "Migrated", "ODTSourceSystem", "IngestionDate", "ValidTo",
]

# Columns used for the final deduplication step
_DEDUP_COLUMNS = [
    "documentId", "filename", "version", "Migrated",
    "originalFilename", "ODTSourceSystem", "IngestionDate",
    "ValidTo", "IsActive",
]


class NsipDocumentHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Document data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-document-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_document"
    HORIZON_TABLE = "odw_standardised_db.document_meta_data"
    AIE_EXTRACTS_TABLE = "odw_harmonised_db.aie_document_data"
    OUTPUT_TABLE = "odw_harmonised_db.nsip_document"
    PRIMARY_KEY = "TEMP_PK"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-document-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load data from:
        - Service bus harmonised table (sb_nsip_document)
        - Horizon standardised table (document_meta_data) joined with AIE extracts
        """
        try:
            self.spark.catalog.refreshTable(self.HORIZON_TABLE)
        except Exception:
            LoggingUtil().log_info(f"Could not refresh table {self.HORIZON_TABLE}, continuing")

        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE} joined with {self.AIE_EXTRACTS_TABLE}")
        horizon_data = self._load_horizon_data()

        # Also load the set of primary keys that exist in the service-bus table.
        # This is used later to derive the Migrated flag.
        sb_primary_keys = self._load_service_bus_primary_keys()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
            "sb_primary_keys": sb_primary_keys,
        }

    def _load_service_bus_data(self) -> DataFrame:
        """
        Get data out of the service bus with additional fields needed for Horizon data.
        Computes a primary key: MD5(CONCAT(documentId, filename, version))
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(documentId, filename, version)) AS {self.PRIMARY_KEY}
                ,NSIPDocumentID
                ,documentId
                ,caseId
                ,caseRef
                ,documentReference
                ,version
                ,examinationRefNo
                ,filename
                ,originalFilename
                ,size
                ,mime
                ,documentURI
                ,publishedDocumentURI
                ,path
                ,virusCheckStatus
                ,fileMD5
                ,dateCreated
                ,lastModified
                ,caseType
                ,redactedStatus
                ,publishedStatus
                ,datePublished
                ,documentType
                ,securityClassification
                ,sourceSystem
                ,origin
                ,owner
                ,author
                ,authorWelsh
                ,representative
                ,description
                ,descriptionWelsh
                ,documentCaseStage
                ,filter1
                ,filter1Welsh
                ,filter2
                ,horizonFolderId
                ,transcriptId

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
        Get data out of Horizon and match the SB schema (with additional fields and ensure data types match).
        LEFT JOINs to aie_document_data on dataid, version, and dataSize.
        Filters to latest expected_from date only.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(Doc.dataId, ''),
                        COALESCE(Doc.name, ''),
                        COALESCE(CAST(Doc.version AS INTEGER), '')
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS LONG) AS NSIPDocumentID
                ,CAST(Doc.dataId AS INTEGER) as documentId
                ,CAST(Doc.casenodeid AS INTEGER) AS caseId
                ,Doc.caseReference AS caseRef
                ,Doc.documentReference
                ,CAST(Doc.version AS INTEGER) AS version
                ,Aie.examinationRefNo
                ,Doc.name as filename
                ,Doc.originalFilename as originalFilename
                ,CAST(Doc.dataSize AS INTEGER) AS size
                ,Aie.mime
                ,Aie.documentURI
                ,CAST(NULL AS String) AS publishedDocumentURI
                ,Aie.path
                ,Doc.virusCheckStatus
                ,Aie.fileMD5
                ,Cast(Doc.createDate as string) as dateCreated
                ,Doc.modifyDate as lastModified
                ,Doc.caseworkType as caseType
                ,CAST(NULL AS String) AS redactedStatus
                ,Doc.publishedStatus
                ,Cast(Doc.datePublished as string) as datePublished
                ,Doc.documentType
                ,Aie.securityClassification
                ,Doc.sourceSystem
                ,Aie.origin AS origin
                ,Aie.owner
                ,Doc.author
                ,Doc.authorWelsh
                ,Doc.representative
                ,Doc.documentDescription AS description
                ,Doc.documentDescriptionWelsh AS descriptionWelsh
                ,Doc.documentCaseStage
                ,Doc.filter1
                ,Doc.filter1Welsh
                ,Doc.filter2
                ,Doc.parentid AS horizonFolderId
                ,Null as transcriptId

                ,"0" as Migrated
                ,"Horizon" as ODTSourceSystem
                ,NULL AS SourceSystemID
                ,to_timestamp(Doc.expected_from) AS IngestionDate
                ,CAST(null as string) as ValidTo
                ,'' as RowID
                ,'Y' as IsActive
            FROM
                {self.HORIZON_TABLE} AS Doc
            LEFT JOIN {self.AIE_EXTRACTS_TABLE} AS Aie
            ON Doc.dataid = Aie.DocumentId
            AND Doc.version = Aie.version
            AND Doc.dataSize = Aie.size
            WHERE
                Doc.expected_from = (SELECT MAX(expected_from) FROM {self.HORIZON_TABLE})
        """)

    def _load_service_bus_primary_keys(self) -> DataFrame:
        """
        Load the distinct set of primary keys from the service-bus table.
        Used to derive the Migrated flag (1 if the PK exists in SB, else 0).
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(documentId, filename, version)) AS {self.PRIMARY_KEY}
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
        sb_primary_keys: DataFrame = self.load_parameter("sb_primary_keys", source_data)

        # Step 1: Align Horizon columns to SB columns and union
        LoggingUtil().log_info(f"Combining data for {self.OUTPUT_TABLE}")
        horizon_data = horizon_data.select(service_bus_data.columns)
        combined = service_bus_data.union(horizon_data)

        # Step 2: Compute RowID from the combined (pre-correction) data.
        row_id_expr = F.md5(
            F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in _ROW_ID_COLUMNS])
        )
        combined = combined.withColumn("RowID", row_id_expr)

        # Step 3: Window-function calculations (replaces intermediate table + SQL views)
        pk = self.PRIMARY_KEY
        win_pk_desc = Window.partitionBy(pk).orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col(pk).asc())

        combined = (
            combined
            .withColumn("ReverseOrderProcessed", F.row_number().over(win_pk_desc))
            .withColumn("NSIPDocumentID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_pk_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        # Step 4: Compute ValidTo by self-joining on (PK, ReverseOrderProcessed - 1)
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = (
            current
            .join(
                next_row,
                (F.col("CurrentRow." + pk) == F.col("NextRow." + pk))
                & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
                "left_outer",
            )
            .select(
                F.col("CurrentRow.NSIPDocumentID").alias("NSIPDocumentID"),
                F.col("CurrentRow." + pk).alias(pk),
                F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
                F.coalesce(
                    F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                    F.col("NextRow.IngestionDate"),
                ).alias("ValidTo"),
                F.col("CurrentRow.IsActive").alias("IsActive"),
            )
        )

        # Step 5: Derive Migrated flag (1 if PK exists in SB table, else 0)
        sb_keys = sb_primary_keys.withColumnRenamed(pk, "sb_pk")
        calcs = (
            calcs
            .join(sb_keys, calcs[pk] == sb_keys["sb_pk"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_pk").isNotNull(), F.lit("1")).otherwise(F.lit("0")))
            .drop("sb_pk")
        )

        # Step 6: Rejoin calculations back onto the combined dataset.
        # Mirror the notebook: take SELECT DISTINCT of all columns from the
        # intermediate data, drop the four columns that calcs will replace,
        # then join calcs back and .select(columns) to restore the full column set.
        all_columns = [c for c in combined.columns if c != "ReverseOrderProcessed"]
        columns = all_columns  # the final column order we want after the join
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("NSIPDocumentID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col(pk).alias(f"calc_{pk}"),
            F.col("IngestionDate").alias("calc_IngestionDate"),
            F.col("NSIPDocumentID"),
            F.col("ValidTo"),
            F.col("Migrated"),
            F.col("IsActive"),
        )

        joined = base.join(
            calcs_renamed,
            (base[pk] == calcs_renamed[f"calc_{pk}"])
            & (base["IngestionDate"] == calcs_renamed["calc_IngestionDate"]),
        ).select(columns)

        # Step 7: Drop the temporary primary key and deduplicate
        final_df = joined.drop(pk).dropDuplicates()
        final_df = final_df.dropDuplicates(subset=_DEDUP_COLUMNS)

        # Ensure final column order: put SourceSystemID where it belongs if present
        # (The base run() -> write_data() will handle the actual write)
        insert_count = final_df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "nsip_document",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "nsip_document",
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





