from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealHasCuratedMipinsProcess(CurationProcess):
    OUTPUT_TABLE = "appeals_has_curated_mipins"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_has_curated_mipins_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasCuratedMipinsProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasCuratedMipinsProcess.process() has not been implemented yet.")
