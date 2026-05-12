from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class NsipInvoiceCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "nsip_invoice"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "nsip_invoice_curated_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("NsipInvoiceCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("NsipInvoiceCuratedProcess.process() has not been implemented yet.")
