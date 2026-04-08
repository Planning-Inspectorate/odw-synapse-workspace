from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
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
        """
        Load source data for appeal representation from the harmonised layer.
        No transformations here, only the basic filter IsActive = 'Y'.
        """
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
