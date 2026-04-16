from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLResultMetadata
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class NsipDocumentCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Document data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-document-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.nsip_document"
    CURATED_PROJECT_TABLE = "odw_curated_db.nsip_project"
    OUTPUT_TABLE = "odw_curated_db.nsip_document"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-document-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Document data from {self.HARMONISED_TABLE}")
        harmonised_docs = self.spark.sql(f"""
            SELECT
                documentId,
                caseId,
                caseRef,
                documentReference,
                version,
                examinationRefNo,
                filename,
                originalFilename,
                size,
                mime,
                documentURI,
                publishedDocumentURI,
                path,
                virusCheckStatus,
                fileMD5,
                dateCreated,
                lastModified,
                caseType,
                redactedStatus,
                PublishedStatus,
                datePublished,
                documentType,
                securityClassification,
                sourceSystem,
                origin,
                owner,
                author,
                authorWelsh,
                representative,
                description,
                descriptionWelsh,
                documentCaseStage,
                filter1,
                filter1Welsh,
                filter2,
                horizonFolderId,
                transcriptId
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        LoggingUtil().log_info(f"Loading curated NSIP Project data from {self.CURATED_PROJECT_TABLE}")
        curated_projects = self.spark.sql(f"""
            SELECT caseReference
            FROM {self.CURATED_PROJECT_TABLE}
        """)

        return {
            "harmonised_docs": harmonised_docs,
            "curated_projects": curated_projects,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Joins and business rules (COALESCE, LOWER, CASE) are applied here.
        No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_docs: DataFrame = self.load_parameter("harmonised_docs", source_data)
        curated_projects: DataFrame = self.load_parameter("curated_projects", source_data)

        # LEFT JOIN to curated projects (matching notebook: LEFT JOIN on caseReference = caseRef)
        docs = harmonised_docs.join(curated_projects, curated_projects["caseReference"] == harmonised_docs["caseRef"], "left")

        # Apply curated column transformations and SELECT DISTINCT
        df = docs.select(
            F.col("documentId"),
            F.col("caseId"),
            F.col("caseRef"),
            F.col("documentReference"),
            F.col("version"),
            F.col("examinationRefNo"),
            F.col("filename"),
            F.col("originalFilename"),
            F.col("size"),
            F.col("mime"),
            F.coalesce(F.col("documentURI"), F.lit("")).alias("documentURI"),
            F.col("publishedDocumentURI"),
            F.col("path"),
            F.col("virusCheckStatus"),
            F.col("fileMD5"),
            F.col("dateCreated"),
            F.col("lastModified"),
            F.lower(F.col("caseType")).alias("caseType"),
            F.col("redactedStatus"),
            F.when(F.col("PublishedStatus") == "Depublished", F.lit("unpublished"))
            .otherwise(F.regexp_replace(F.lower(F.col("PublishedStatus")), " ", "_"))
            .alias("publishedStatus"),
            F.col("datePublished"),
            F.col("documentType"),
            F.col("securityClassification"),
            F.col("sourceSystem"),
            F.col("origin"),
            F.col("owner"),
            F.col("author"),
            F.col("authorWelsh"),
            F.col("representative"),
            F.col("description"),
            F.col("descriptionWelsh"),
            F.when(F.col("documentCaseStage") == "Developer's Application", F.lit("developers_application"))
            .when(F.col("documentCaseStage") == "Post decision", F.lit("post_decision"))
            .otherwise(F.lower(F.col("documentCaseStage")))
            .alias("documentCaseStage"),
            F.col("filter1"),
            F.col("filter1Welsh"),
            F.col("filter2"),
            F.col("horizonFolderId"),
            F.col("transcriptId"),
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Document row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "nsip_document",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "nsip_document",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResultMetadata(
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
