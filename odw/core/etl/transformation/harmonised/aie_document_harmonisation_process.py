from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Columns used to build the MD5 RowID hash, in the order defined by the original notebook.
# Note: AIEDocumentDataID is NULL at the point RowID is computed (before calcs are applied),
# so it always contributes '.' to the hash — this preserves backward-compatible RowID values.
_AIE_DOCUMENT_ROW_ID_COLUMNS = [
    "AIEDocumentDataID",
    "documentId",
    "caseRef",
    "documentReference",
    "version",
    "examinationRefNo",
    "filename",
    "originalFilename",
    "size",
    "mime",
    "documentUri",
    "path",
    "virusCheckStatus",
    "fileMD5",
    "dateCreated",
    "lastModified",
    "caseType",
    "documentStatus",
    "redactedStatus",
    "publishedStatus",
    "datePublished",
    "documentType",
    "securityClassification",
    "sourceSystem",
    "origin",
    "owner",
    "author",
    "representative",
    "description",
    "stage",
    "filter1",
    "filter2",
    "Migrated",
    "ODTSourceSystem",
    "IngestionDate",
    "ValidTo",
]


class AieDocumentHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising AIE Document data from the Horizon standardised table.

    Implements the Load → Transform → Write pattern, replacing the ad-hoc cell logic
    previously contained in py_horizon_harmonised_aie_document.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "aie-document-harmonised",
        "debug": False
    }
    ```
    """

    HORIZON_TABLE = "odw_standardised_db.aie_document_data"
    OUTPUT_TABLE = "odw_harmonised_db.aie_document_data"
    PRIMARY_KEY = "TEMP_PK"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "AIE Harmonisation"

    # ------------------------------------------------------------------
    # load_data – all reads happen here, no joins or transformations
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load the latest AIE document records from the Horizon standardised table.
        Filtering to the most recent expected_from date is the only operation applied here.
        """
        LoggingUtil().log_info(f"Loading AIE document data from {self.HORIZON_TABLE}")
        return {"horizon_data": self._load_horizon_data()}

    def _load_horizon_data(self) -> DataFrame:
        """
        Read Horizon AIE document data filtered to the latest expected_from date.
        Computes the composite primary key TEMP_PK = MD5(documentId, filename, version)
        and seeds the derived columns (AIEDocumentDataID, ValidTo, RowID, IsActive)
        with their initial values so that process() can work against a stable schema.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(CAST(documentId AS INTEGER), ''),
                        COALESCE(filename, ''),
                        COALESCE(CAST(version AS INTEGER), '')
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS LONG) AS AIEDocumentDataID
                ,documentId
                ,caseRef
                ,documentReference
                ,version
                ,examinationRefNo
                ,filename
                ,originalFilename
                ,size
                ,mime
                ,documentUri
                ,path
                ,virusCheckStatus
                ,fileMD5
                ,dateCreated
                ,lastModified
                ,caseType
                ,documentStatus
                ,redactedStatus
                ,publishedStatus
                ,datePublished
                ,documentType
                ,securityClassification
                ,sourceSystem
                ,origin
                ,owner
                ,author
                ,representative
                ,description
                ,stage
                ,filter1
                ,filter2
                ,'0' AS Migrated
                ,'Horizon' AS ODTSourceSystem
                ,to_timestamp(expected_from) AS IngestionDate
                ,CAST(null AS string) AS ValidTo
                ,'' AS RowID
                ,'Y' AS IsActive
            FROM {self.HORIZON_TABLE}
            WHERE expected_from = (SELECT MAX(expected_from) FROM {self.HORIZON_TABLE})
        """)

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply all harmonisation transformations:

        1. Compute RowID from the loaded data (AIEDocumentDataID is NULL at this point,
           which matches the original notebook behaviour).
        2. Derive AIEDocumentDataID, IsActive, and ReverseOrderProcessed via window functions.
        3. Self-join to populate ValidTo for historical records.
        4. Rejoin the calculated columns back onto the base dataset.
        5. Drop TEMP_PK and deduplicate.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)

        pk = self.PRIMARY_KEY

        # Step 1: Compute RowID while AIEDocumentDataID is still NULL.
        # This mirrors the notebook, where RowID is generated from the intermediate
        # table before the calcs view is applied.
        row_id_expr = F.md5(
            F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in _AIE_DOCUMENT_ROW_ID_COLUMNS])
        )
        data_with_row_id = horizon_data.withColumn("RowID", row_id_expr)

        # Step 2: Window-function calculations (replaces the SQL view pattern from the notebook)
        win_pk_desc = Window.partitionBy(pk).orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col(pk).asc())

        LoggingUtil().log_info("Computing AIEDocumentDataID and IsActive flags")
        combined = (
            data_with_row_id
            .withColumn("ReverseOrderProcessed", F.row_number().over(win_pk_desc))
            .withColumn("AIEDocumentDataID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_pk_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        # Step 3: Compute ValidTo by self-joining on (PK, ReverseOrderProcessed - 1 = ReverseOrderProcessed).
        # This fills ValidTo for historical records with the IngestionDate of the next version.
        LoggingUtil().log_info("Computing ValidTo for historical records")
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col("CurrentRow." + pk) == F.col("NextRow." + pk))
            & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.AIEDocumentDataID").alias("AIEDocumentDataID"),
            F.col("CurrentRow." + pk).alias(pk),
            F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.lit("0").alias("Migrated"),
            F.col("CurrentRow.IsActive").alias("IsActive"),
        )

        # Step 4: Rejoin calcs onto the base data.
        # Mirror the notebook: select all non-helper columns, drop the four that calcs will
        # replace, join calcs back, then restore the original column order via .select().
        all_columns = [c for c in combined.columns if c != "ReverseOrderProcessed"]
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("AIEDocumentDataID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col(pk).alias(f"calc_{pk}"),
            F.col("IngestionDate").alias("calc_IngestionDate"),
            F.col("AIEDocumentDataID"),
            F.col("ValidTo"),
            F.col("Migrated"),
            F.col("IsActive"),
        )

        LoggingUtil().log_info("Merging and de-duplicating data")
        joined = base.join(
            calcs_renamed,
            (base[pk] == calcs_renamed[f"calc_{pk}"]) & (base["IngestionDate"] == calcs_renamed["calc_IngestionDate"]),
        ).select(all_columns)

        # Step 5: Drop the temporary primary key and deduplicate
        final_df = joined.drop(pk).drop_duplicates()

        insert_count = final_df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "aie_document_data",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "aie_document_data",
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
