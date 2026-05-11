from typing import Any, Dict, Tuple
from datetime import datetime

from pyspark.sql import DataFrame, functions as F

from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLSuccessResult
from odw.core.util.logging_util import LoggingUtil

class NsipInvoiceCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "nsip_invoice"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
        self.logger = LoggingUtil(self.__class__.__name__)

    def get_name(self) -> str:
        return "nsip_invoice_curated_process"

    def load_data(self) -> Dict[str, Any]:
        source_df = self.spark.table("odw_harmonised_db.sb_nsip_invoice")
        target_exists = self.spark._jsparkSession.catalog().tableExists(
            "odw_curated_db.nsip_invoice"
        )
        return {
            "source_data": source_df,
            "target_exists": target_exists,
        }

    def process(self, source_data: Dict[str, Any]) -> Tuple[Dict[str, Any], ETLSuccessResult]:
        logger = LoggingUtil(self.__class__.__name__)
       
        start_time = datetime.now()

        harmonised_df: DataFrame = source_data["source_data"]

        active_df = harmonised_df.where(F.col("IsActive") == F.lit("Y"))

        curated_columns = [
            "caseId",
            "caseReference",
            "invoiceStage",
            "invoiceNumber",
            "amountDue",
            "paymentDueDate",
            "invoicedDate",
            "paymentDate",
            "refundCreditNoteNumber",
            "refundAmount",
            "IsActive",
        ]
        projected_df = active_df.select(*[F.col(c) for c in curated_columns])

        distinct_df = projected_df.distinct()

        insert_count = distinct_df.count()

        data_to_write: Dict[str, Any] = {
            self.OUTPUT_TABLE: {
                "data": distinct_df,
                "write_mode": "overwrite",
            }
        }

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = ETLSuccessResult(
            metadata={
                "insert_count": insert_count,
                "update_count": 0,
                "start_execution_time": start_time.isoformat(),
                "end_execution_time": end_time.isoformat(),
                "activity_type": "curated_transformation",
                "duration_seconds": duration,
            }
        )

        logger.info(
            f"NsipInvoiceCuratedProcess: prepared {insert_count} curated rows for table "
            f"odw_curated_db.{self.OUTPUT_TABLE}"
        )

        return data_to_write, result
