from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLResultMetadata
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class AppealRepresentationCuratedProcess(CurationProcess):

    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_representation"
    OUTPUT_TABLE = "odw_curated_db.appeal_representation"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal-representation-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(
            f"Loading harmonised Appeal Representation data from {self.HARMONISED_TABLE}"
        )

        harmonised_representations = self.spark.sql(
            f"""
            SELECT
                representationId,
                caseId,
                caseReference,
                representationStatus,
                originalRepresentation,
                redacted,
                redactedRepresentation,
                redactedBy,
                invalidOrIncompleteDetails,
                otherInvalidOrIncompleteDetails,
                source,
                serviceUserId,
                representationType,
                dateReceived,
                documentIds
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
            """
        )

        return {
            "harmonised_representations": harmonised_representations,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_representations: DataFrame = self.load_parameter(
            "harmonised_representations", source_data
        )

        df = harmonised_representations

        insert_count = df.count()
        LoggingUtil().log_info(
            f"Curated Appeal Representation row count: {insert_count}"
        )

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_representation",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "appeal_representation",
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
