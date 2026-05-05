<<<<<<< HEAD
from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealServiceUserCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_service_user"
    SOURCE_TABLE = "service_user"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_service_user_curated_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealServiceUserCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealServiceUserCuratedProcess.process() has not been implemented yet.")
=======
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple


class AppealServiceUserCuratedProcess(CurationProcess):
    """
    ETL process for building curated appeal service user data.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal-service-user-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.service_user"
    OUTPUT_TABLE = "odw_curated_db.appeal_service_user"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal-service-user-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading harmonised service user data from {self.HARMONISED_TABLE}")
        harmonised_service_user = self.spark.sql(f"""
            SELECT
                id,
                salutation,
                firstName,
                lastName,
                addressLine1,
                addressLine2,
                addressTown,
                addressCounty,
                postcode,
                addressCountry,
                organisation,
                organisationType,
                role,
                telephoneNumber,
                otherPhoneNumber,
                faxNumber,
                emailAddress,
                webAddress,
                serviceUserType,
                caseReference,
                sourceSuid,
                sourceSystem
            FROM {self.HARMONISED_TABLE}
        """)

        return {
            "harmonised_service_user": harmonised_service_user,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_service_user: DataFrame = self.load_parameter("harmonised_service_user", source_data)

        df = harmonised_service_user.filter(
            (harmonised_service_user["sourceSystem"] == "back-office-appeals")
            | (harmonised_service_user["serviceUserType"] == "Appellant")
        )

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated appeal service user row count: {insert_count}")

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_service_user",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "odw_curated_db.appeal_service_user",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
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
>>>>>>> 925b68494 (3 notebooks were refactored and updated etl_process_factory.py file)
