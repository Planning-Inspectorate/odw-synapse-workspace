from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
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
        Load raw source data from the harmonised and curated layers.
        No transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Document data from {self.HARMONISED_TABLE}")
        harmonised_docs = self.spark.sql(f"SELECT * FROM {self.HARMONISED_TABLE}")

        LoggingUtil().log_info(f"Loading curated NSIP Project data from {self.CURATED_PROJECT_TABLE}")
        curated_projects = self.spark.sql(f"SELECT * FROM {self.CURATED_PROJECT_TABLE}")

        return {
            "harmonised_docs": harmonised_docs,
            "curated_projects": curated_projects,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        All business rules (COALESCE, LOWER, CASE transformations, LEFT JOIN, filtering)
        are applied here. No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_docs: DataFrame = self.load_parameter("harmonised_docs", source_data)
        curated_projects: DataFrame = self.load_parameter("curated_projects", source_data)

        # Register temp views so we can use Spark SQL for the transformation
        harmonised_docs.createOrReplaceTempView("harmonised_nsip_document")
        curated_projects.createOrReplaceTempView("curated_nsip_project")

        df = self.spark.sql("""
            SELECT DISTINCT
            doc.documentId,
            doc.caseId,
            doc.caseRef,
            doc.documentReference,
            doc.version,
            doc.examinationRefNo,
            doc.filename,
            doc.originalFilename,
            doc.size,
            doc.mime,
            COALESCE(doc.documentURI, '') AS documentURI,
            doc.publishedDocumentURI,
            doc.path,
            doc.virusCheckStatus,
            doc.fileMD5,
            doc.dateCreated,
            doc.lastModified,
            LOWER(doc.caseType) AS caseType,
            doc.redactedStatus,
            CASE
                WHEN doc.PublishedStatus = 'Depublished'
                THEN 'unpublished'
                ELSE REPLACE(
                    LOWER(doc.PublishedStatus),
                    ' ',
                    '_')
            END AS publishedStatus,
            doc.datePublished,
            doc.documentType,
            doc.securityClassification,
            doc.sourceSystem,
            doc.origin,
            doc.owner,
            doc.author,
            doc.authorWelsh,
            doc.representative,
            doc.description,
            doc.descriptionWelsh,
            CASE
                WHEN doc.documentCaseStage = "Developer's Application"
                THEN 'developers_application'
                WHEN doc.documentCaseStage = 'Post decision'
                THEN 'post_decision'
                ELSE LOWER(doc.documentCaseStage)
            END AS documentCaseStage,
            doc.filter1,
            doc.filter1Welsh,
            doc.filter2,
            doc.horizonFolderId,
            doc.transcriptId
            FROM harmonised_nsip_document AS doc
            LEFT JOIN curated_nsip_project proj
                ON proj.caseReference = doc.caseRef
            WHERE doc.IsActive = 'Y'
        """)

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

