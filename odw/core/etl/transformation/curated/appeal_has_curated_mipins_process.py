from typing import Any, Dict
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from pyspark.sql import DataFrame


class AppealHasCuratedMipinsProcess(CurationProcess):
    HARMONISED_TABLE = "appeal_has"
    OUTPUT_TABLE = "appeals_has_curated_mipins"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal HAS MIPINS Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError(
            "AppealHasCuratedMipinsProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealHasCuratedMipinsProcess.process() has not been implemented yet."
        )
