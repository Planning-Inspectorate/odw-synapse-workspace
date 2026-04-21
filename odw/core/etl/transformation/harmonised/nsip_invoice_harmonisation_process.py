from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class NsipInvoiceHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "sb_nsip_invoice"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "nsip_invoice_harmonisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("NsipInvoiceHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("NsipInvoiceHarmonisationProcess.process() has not been implemented yet.")
