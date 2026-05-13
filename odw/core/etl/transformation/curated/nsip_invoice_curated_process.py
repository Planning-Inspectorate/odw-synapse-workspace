from typing import Dict, Tuple
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.util.logging_util import LoggingUtil


class NsipInvoiceCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Invoice data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-invoice-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.sb_nsip_invoice"
    OUTPUT_TABLE = "odw_curated_db.nsip_invoice"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip_invoice_curated_process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        Filters are applied here for performance (IsActive = 'Y').
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Invoice data from {self.HARMONISED_TABLE}")
        harmonised_nsip_invoice = self.spark.sql(f"""
            SELECT
                caseId,
                caseReference,
                invoiceStage,
                invoiceNumber,
                amountDue,
                paymentDueDate,
                invoicedDate,
                paymentDate,
                refundCreditNoteNumber,
                refundAmount,
                IsActive
            FROM {self.HARMONISED_TABLE}
            WHERE IsActive = 'Y'
        """)

        return {
            "harmonised_nsip_invoice": harmonised_nsip_invoice,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.
        Mirrors the legacy SELECT DISTINCT of the curated business columns
        over active rows. No reads or writes happen in this method.
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)

        harmonised_nsip_invoice: DataFrame = source_data.get("harmonised_nsip_invoice")
        if harmonised_nsip_invoice is None:
            harmonised_nsip_invoice = source_data.get("source_data")

        if harmonised_nsip_invoice is None:
            raise ValueError("NsipInvoiceCuratedProcess requires a harmonised_nsip_invoice parameter to be provided, but was missing")

        df = harmonised_nsip_invoice.select(
            F.col("caseId"),
            F.col("caseReference"),
            F.col("invoiceStage"),
            F.col("invoiceNumber"),
            F.col("amountDue"),
            F.col("paymentDueDate"),
            F.col("invoicedDate"),
            F.col("paymentDate"),
            F.col("refundCreditNoteNumber"),
            F.col("refundAmount"),
            F.col("IsActive"),
        ).distinct()

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Invoice row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "nsip_invoice",
                "storage_endpoint": "",
                "container_name": "odw-curated",
                "blob_path": "nsip_invoice",
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
