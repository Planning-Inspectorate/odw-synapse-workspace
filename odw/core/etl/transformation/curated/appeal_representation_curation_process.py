from odw.core.etl.transformation.curated.curation_process import CurationProcess
from typing import Any


class AppealRepresentationCurationProcess(CurationProcess):
    HARMONISED_TABLE = "sb_appeal_representation"
    CURATED_TABLE = "appeal_representation"

    def get_name(self) -> str:
        return "Appeal Representation Curation"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealRepresentationCurationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealRepresentationCurationProcess.process() has not been implemented yet.")
