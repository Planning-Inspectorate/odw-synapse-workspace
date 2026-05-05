<<<<<<< HEAD
from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealServiceUserCuratedMipinsProcess(CurationProcess):
    SOURCE_SB_SERVICE_USER_TABLE = "sb_service_user"
    SOURCE_SERVICE_USER_TABLE = "service_user"
    OUTPUT_TABLE = "appeal_service_user_curated_mipins"

    def get_name(self) -> str:
        return "appeal_service_user_curated_mipins_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealServiceUserCuratedMipinsProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealServiceUserCuratedMipinsProcess.process() has not been implemented yet.")
=======
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class AppealServiceUserCuratedMipinsProcess(CurationProcess):
    """
    ETL process for building the curated MiPINS appeal service user table.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "appeal-service-user-curated-mipins",
        "debug": False
    }
    ```
    """

    SOURCE_SB_TABLE = "odw_harmonised_db.sb_service_user"
    SOURCE_SERVICE_USER_TABLE = "odw_harmonised_db.service_user"
    OUTPUT_TABLE = "odw_curated_db.appeal_service_user_curated_mipins"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "appeal-service-user-curated-mipins"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading harmonised service bus data from {self.SOURCE_SB_TABLE}")
        sb_service_user = self.spark.sql(f"""
            SELECT
                id,
                caseReference,
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
                sourceSystem,
                sourceSUID,
                ODTSourceSystem,
                ingestionDate,
                NULLIF(ValidTo, '') AS ValidTo,
                RowID,
                IsActive
            FROM {self.SOURCE_SB_TABLE}
        """)

        LoggingUtil().log_info(f"Loading harmonised service user data from {self.SOURCE_SERVICE_USER_TABLE}")
        service_user = self.spark.sql(f"""
            SELECT
                id,
                caseReference,
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
                sourceSystem,
                sourceSUID,
                ODTSourceSystem,
                ingestionDate,
                ValidTo,
                RowID,
                IsActive
            FROM {self.SOURCE_SERVICE_USER_TABLE}
        """)

        return {
            "sb_service_user": sb_service_user,
            "service_user": service_user,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        sb_service_user: DataFrame = self.load_parameter("sb_service_user", source_data)
        service_user: DataFrame = self.load_parameter("service_user", source_data)

        min_date = F.to_timestamp(F.lit("1900-01-01"))

        sb_filtered = sb_service_user.filter(
            (F.col("ingestionDate").isNull() | (F.to_timestamp(F.col("ingestionDate")) >= min_date))
            & (F.col("ValidTo").isNull() | (F.to_timestamp(F.col("ValidTo")) >= min_date))
        )

        service_user_filtered = service_user.filter(
            (F.col("ingestionDate").isNull() | (F.to_timestamp(F.col("ingestionDate")) >= min_date))
            & (F.col("ValidTo").isNull() | (F.to_timestamp(F.col("ValidTo")) >= min_date))
        )

        df = sb_filtered.unionByName(service_user_filtered, allowMissingColumns=True).dropDuplicates()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated MiPINS appeal service user row count: {insert_count}")

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "appeal_service_user_curated_mipins",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "odw_curated_db.appeal_service_user_curated_mipins",
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
