from typing import Dict, Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.util import Util
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from datetime import datetime


class NsipInvoiceCuratedProcess(CurationProcess):
    HARMONISED_TABLE = "sb_nsip_invoice"
    OUTPUT_TABLE = "nsip_invoice"

    @classmethod
    def get_name(cls) -> str:
        return "NSIP Invoice Curation Process"

    def load_data(self, **kwargs) -> Dict[str, Any]:
        nsip_invoices = self.spark.sql(f"SELECT * FROM odw_harmonised_db.{self.HARMONISED_TABLE}")
        return {"source_data": nsip_invoices}

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        nsip_invoices: DataFrame = self.load_parameter("source_data", source_data)
        active_nsip_invoices = (
            nsip_invoices.select(
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
                "refundIssueDate",
            )
            .distinct()
            .filter(F.col("IsActive") == "Y")
            .drop("IsActive")
        )

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": active_nsip_invoices,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": self.OUTPUT_TABLE,
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
        insert_count = active_nsip_invoices.count()
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
