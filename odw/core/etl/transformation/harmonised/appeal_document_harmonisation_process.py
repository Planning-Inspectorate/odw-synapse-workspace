from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class AppealDocumentHarmonisationProcess(HarmonisationProcess):
    SERVICE_BUS_TABLE = "sb_appeal_document"
    HORIZON_TABLE = "horizon_appeals_document_metadata"
    AIE_TABLE = "aie_document_data"
    OUTPUT_TABLE = "appeal_document"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_document_harmonisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError(
            "AppealDocumentHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealDocumentHarmonisationProcess.process() has not been implemented yet."
        )

    def _generate_rowid(self, row: dict[str, Any]) -> str:
        raise NotImplementedError(
            "AppealDocumentHarmonisationProcess._generate_rowid() has not been implemented yet."
        )