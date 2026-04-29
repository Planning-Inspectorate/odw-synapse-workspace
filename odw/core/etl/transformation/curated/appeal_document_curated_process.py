from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class AppealDocumentCuratedProcess(CurationProcess):
    """
    ETL process for curating Appeal Document data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal_document_curated_process",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.appeal_document"
    OUTPUT_TABLE = "odw_curated_db.appeal_document"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal_document_curated_process"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        """

        LoggingUtil().log_info(f"Loading harmonised Appeal Document data from {self.HARMONISED_TABLE}")

        harmonised_appeal_docs = self.spark.sql(f"""
            SELECT DISTINCT
                documentId,
                caseId,
                caseReference,
                version,
                filename,
                originalFilename,
                size,
                COALESCE(mime, '') AS mime,
                COALESCE(documentURI, '') AS documentURI,
                publishedDocumentURI,
                virusCheckStatus,
                fileMD5,
                dateCreated,
                dateReceived,
                datePublished,
                lastModified,
                caseType,
                redactedStatus,
                documentType,
                sourceSystem,
                origin,
                owner,
                author,
                description,
                caseStage,
                horizonFolderId
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)
        return {
            "harmonised_appeal_docs": harmonised_appeal_docs,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        All business rules (caseType mapping, COALESCE) are applied here.
        No reads or writes happen in this method.
        """

        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_appeal_docs: DataFrame = self.load_parameter("harmonised_appeal_docs", source_data)

        # Apply curated column transformations and SELECT DISTINCT
        df = harmonised_appeal_docs.select(
            F.col("documentId"),
            F.col("caseId"),
            F.col("caseReference"),
            F.col("version"),
            F.col("filename"),
            F.col("originalFilename"),
            F.col("size"),
            F.coalesce(F.col("mime"), F.lit("")).alias("mime"),
            F.coalesce(F.col("documentURI"), F.lit("")).alias("documentURI"),
            F.col("publishedDocumentURI"),
            F.col("virusCheckStatus"),
            F.col("fileMD5"),
            F.col("dateCreated"),
            F.col("dateReceived"),
            F.col("datePublished"),
            F.col("lastModified"),
            # caseType mapping
            F.when(F.col("caseType") == "Planning Listed Building and Conservation Area Appeal (Y)", F.lit("Y"))
            .when(F.col("caseType") == "Lawful Development Certificate Appeal", F.lit("X"))
            .when(F.col("caseType") == "Planning Obligation Appeal", F.lit("Q"))
            .when(F.col("caseType") == "Commercial (CAS) Appeal", F.lit("Z"))
            .when(F.col("caseType") == "Enforcement Listed Building and Conservation Area Appeal", F.lit("F"))
            .when(F.col("caseType") == "Advertisement Appeal", F.lit("H"))
            .when(F.col("caseType") == "Planning Appeal (A)", F.lit("W"))
            .when(F.col("caseType") == "Enforcement Notice Appeal", F.lit("C"))
            .when(F.col("caseType") == "Discontinuance Notice Appeal", F.lit("G"))
            .when(F.col("caseType") == "Community Infrastructure Levy", F.lit("L"))
            .when(F.col("caseType") == "Planning Appeal (W)", F.lit("W"))
            .when(F.col("caseType") == "Call-In Application", F.lit("V"))
            .when(F.col("caseType") == "Affordable Housing Obligation Appeal", F.lit("S"))
            .when(F.col("caseType") == "Householder (HAS) Appeal", F.lit("D"))
            .otherwise(F.col("caseType"))
            .alias("caseType"),
            F.col("redactedStatus"),
            F.col("documentType"),
            F.col("sourceSystem"),
            F.col("origin"),
            F.col("owner"),
            F.col("author"),
            F.col("description"),
            F.col("caseStage"),
            F.col("horizonFolderId"),
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated Appeal Document row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_document",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "appeal_document",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
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
