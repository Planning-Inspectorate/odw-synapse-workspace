from typing import Any

from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealFolderCurationProcess(CurationProcess):
    HARMONISED_TABLE = "odw_harmonised_db.appeals_folder"
    OUTPUT_TABLE = "odw_curated_db.appeal_folder"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal Folder Curation Process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealFolderCurationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealFolderCurationProcess.process() has not been implemented yet.")
