from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealHasCuratedMipinsProcess(CurationProcess):
    HARMONISED_TABLE = "appeal_has"
    OUTPUT_TABLE = "appeals_has_curated_mipins"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal HAS MIPINS Curation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasCuratedMipinsProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasCuratedMipinsProcess.process() has not been implemented yet.")
