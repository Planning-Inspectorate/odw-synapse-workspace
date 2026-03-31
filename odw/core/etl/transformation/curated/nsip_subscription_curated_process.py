from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple


class NsipSubscriptionCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Subscription data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-subscription-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_nsip_subscription"
    OUTPUT_TABLE = "odw_curated_db.nsip_subscription"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-subscription-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        No transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Subscription data from {self.HARMONISED_TABLE}")
        harmonised_subscriptions = self.spark.sql(f"""
            SELECT
                subscriptionId,
                caseReference,
                emailAddress,
                subscriptionType,
                startDate,
                endDate,
                language
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        return {
            "harmonised_subscriptions": harmonised_subscriptions,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        For subscriptions this is a simple filter + SELECT DISTINCT.
        No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_subscriptions: DataFrame = self.load_parameter("harmonised_subscriptions", source_data)

        # Select curated columns and deduplicate
        df = harmonised_subscriptions.select(
            "subscriptionId",
            "caseReference",
            "emailAddress",
            "subscriptionType",
            "startDate",
            "endDate",
            "language",
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Subscription row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "nsip_subscription",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "nsip_subscription",
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
