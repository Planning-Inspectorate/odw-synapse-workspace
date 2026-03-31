from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class NsipRepresentationCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Representation data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-representation-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.nsip_representation"
    OUTPUT_TABLE = "odw_curated_db.nsip_representation"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-representation-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        Filters are applied here for performance (IsActive, caseRef IS NOT NULL).
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Representation data from {self.HARMONISED_TABLE}")
        harmonised_representations = self.spark.sql(f"""
            SELECT
                representationId,
                webreference,
                examinationLibraryRef,
                caseRef,
                caseId,
                status,
                originalRepresentation,
                redacted,
                redactedRepresentation,
                redactedBy,
                redactedNotes,
                representationFrom,
                representedId,
                representativeId,
                registerFor,
                representationType,
                dateReceived,
                attachmentIds
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
                AND caseRef IS NOT NULL
        """)

        return {
            "harmonised_representations": harmonised_representations,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        All business rules (status mapping, representationFrom mapping, registerFor mapping,
        representationType mapping, COALESCE) are applied here.
        No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_representations: DataFrame = self.load_parameter("harmonised_representations", source_data)

        # Apply curated column transformations and SELECT DISTINCT
        df = harmonised_representations.select(
            F.col("representationId"),
            F.col("webreference").alias("referenceId"),
            F.col("examinationLibraryRef"),
            F.col("caseRef"),
            F.col("caseId"),
            # status mapping
            F.when((F.col("status") == "New") | (F.col("status") == "In Progress"), F.lit("awaiting_review"))
            .when(F.col("status") == "Complete", F.lit("valid"))
            .when(F.col("status") == "Do Not Publish", F.lit("invalid"))
            .otherwise(F.lower(F.col("status")))
            .alias("status"),
            F.coalesce(F.col("originalRepresentation"), F.lit("")).alias("originalRepresentation"),
            F.col("redacted"),
            F.col("redactedRepresentation"),
            F.col("redactedBy"),
            F.col("redactedNotes"),
            # representationFrom mapping
            F.when(F.col("representationFrom") == "An Organisation", F.lit("ORGANISATION"))
            .when(F.col("representationFrom") == "Members of the Public/Businesses", F.lit("PERSON"))
            .when(F.col("representationFrom") == "Another Individual or Organisation", F.lit("PERSON"))
            .when(F.col("representationFrom") == "Myself", F.lit("PERSON"))
            .otherwise(F.col("representationFrom"))
            .alias("representationFrom"),
            F.col("representedId"),
            F.col("representativeId"),
            # registerFor mapping (same as representationFrom but fallback to registerFor)
            F.when(F.col("representationFrom") == "An Organisation", F.lit("ORGANISATION"))
            .when(F.col("representationFrom") == "Members of the Public/Businesses", F.lit("PERSON"))
            .when(F.col("representationFrom") == "Another Individual or Organisation", F.lit("PERSON"))
            .when(F.col("representationFrom") == "Myself", F.lit("PERSON"))
            .otherwise(F.col("registerFor"))
            .alias("registerFor"),
            # representationType mapping
            F.when(F.col("representationType") == "Other Statutory Consultees", F.lit("Statutory Consultees"))
            .otherwise(F.col("representationType"))
            .alias("representationType"),
            F.col("dateReceived"),
            F.col("attachmentIds"),
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Representation row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "nsip_representation",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "nsip_representation",
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
