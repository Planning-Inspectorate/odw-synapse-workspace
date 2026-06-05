from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealHasCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_has"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "Appeal HAS Curation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasCuratedProcess.process() has not been implemented yet.")
