from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealEventEstimateCuratedMipinsProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_event_estimate_curated_mipins"
    SOURCE_TABLE = "sb_appeal_event_estimate"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_event_estimate_curated_mipins_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealEventEstimateCuratedMipinsProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealEventEstimateCuratedMipinsProcess.process() has not been implemented yet.")