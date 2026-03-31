from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class NsipS51AdviceCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP S51 Advice data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-s51-advice-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.nsip_s51_advice"
    OUTPUT_TABLE = "odw_curated_db.s51_advice"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-s51-advice-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        Filters are applied here for performance (IsActive = 'Y').
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP S51 Advice data from {self.HARMONISED_TABLE}")
        harmonised_s51_advice = self.spark.sql(f"""
            SELECT
                adviceId,
                adviceReference,
                caseId,
                caseReference,
                title,
                titleWelsh,
                `from`,
                agent,
                method,
                enquiryDate,
                enquiryDetails,
                enquiryDetailsWelsh,
                adviceGivenBy,
                adviceDate,
                adviceDetails,
                adviceDetailsWelsh,
                status,
                redactionStatus,
                attachmentIds
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        return {
            "harmonised_s51_advice": harmonised_s51_advice,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        All business rules (caseId mapping, agent/method NULL handling,
        status mapping) are applied here.
        No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_s51_advice: DataFrame = self.load_parameter("harmonised_s51_advice", source_data)

        # Apply curated column transformations and SELECT DISTINCT
        df = harmonised_s51_advice.select(
            F.col("adviceId"),
            F.col("adviceReference"),
            # caseId: 'None' -> -1 (INT), else keep
            F.when(F.col("caseId") == "None", F.lit(-1).cast("int")).otherwise(F.col("caseId")).alias("caseId"),
            F.col("caseReference"),
            F.col("title"),
            F.col("titleWelsh"),
            F.col("`from`"),
            # agent: LOWER = 'none' -> NULL, else keep
            F.when(F.lower(F.col("agent")) == "none", F.lit(None)).otherwise(F.col("agent")).alias("agent"),
            # method: LOWER = 'none' -> NULL, else LOWER
            F.when(F.lower(F.col("method")) == "none", F.lit(None)).otherwise(F.lower(F.col("method"))).alias("method"),
            F.col("enquiryDate"),
            F.col("enquiryDetails"),
            F.col("enquiryDetailsWelsh"),
            F.col("adviceGivenBy"),
            F.col("adviceDate"),
            F.col("adviceDetails"),
            F.col("adviceDetailsWelsh"),
            # status mapping
            F.when(F.col("status").isin("Not Checked", "unchecked", "Depublished"), F.lit("unchecked"))
            .when(F.col("status") == "Do Not Publish", F.lit("donotpublish"))
            .otherwise(F.lower(F.col("status")))
            .alias("status"),
            F.col("redactionStatus"),
            F.col("attachmentIds"),
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP S51 Advice row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "s51_advice",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "s51_advice",
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
