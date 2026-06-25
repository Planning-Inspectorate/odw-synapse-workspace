from typing import Any, Dict
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from pyspark.sql import DataFrame


class AppealHasCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_has"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal HAS Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("AppealHasCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasCuratedProcess.process() has not been implemented yet.")
