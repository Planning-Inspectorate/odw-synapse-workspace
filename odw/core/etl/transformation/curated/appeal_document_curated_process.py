from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealDocumentCuratedProcess(CurationProcess):
    SOURCE_TABLE = "appeal_document"
    OUTPUT_TABLE = "appeal_document"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_document_curated_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealDocumentCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealDocumentCuratedProcess.process() has not been implemented yet.")
