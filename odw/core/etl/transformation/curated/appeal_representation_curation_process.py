from datetime import datetime
from typing import Dict, Tuple
<<<<<<< Updated upstream
=======

from pyspark.sql import DataFrame, SparkSession

from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.util import Util
from odw.test.util.session_util import PytestSparkSessionUtil
>>>>>>> Stashed changes

from pyspark.sql import DataFrame, SparkSession

from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.util import Util
from odw.test.util.session_util import PytestSparkSessionUtil

class AppealRepresentationCurationProcess(CurationProcess):
    """Curates active harmonised appeal representation data into the curated layer."""

<<<<<<< Updated upstream
    HARMONISED_TABLE = "odw_harmonised_db.tu_ar_ld__harmonised"
    CURATED_TABLE = "odw_curated_db.tu_ar_ld__curated"
=======
    HARMONISED_TABLE = "odw_harmonised_db.sb_appeal_representation"
    CURATED_TABLE = "odw_curated_db.appeal_representation"
>>>>>>> Stashed changes
    OUTPUT_TABLE = "odw_curated_db.appeal_representation"

    def __init__(self, spark: SparkSession = None, debug: bool = False):
        if spark is not None:
            super().__init__(spark, debug)
        else:
            self.spark = None
            self.debug = debug

    @classmethod
    def get_name(cls) -> str:
        return "appeal-representation-curated"

    def _get_spark(self) -> SparkSession:
        spark = getattr(self, "spark", None)
        if spark is None:
            spark = PytestSparkSessionUtil().get_spark_session()
        return spark

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        spark = self._get_spark()

        harmonised_data = spark.sql(
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

        curated_data_description = spark.sql(
            f"DESCRIBE DETAIL {self.CURATED_TABLE}"
        )

        return {
            "harmonised_data": harmonised_data,
            "curated_data_description": curated_data_description,
        }

    def process(self, source_data=None, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        if source_data is None:
            source_data = self.load_parameter("source_data", kwargs)

        harmonised_data: DataFrame = source_data["harmonised_data"]
        df = harmonised_data

        insert_count = df.count()
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
