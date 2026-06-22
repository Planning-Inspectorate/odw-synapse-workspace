from typing import Any

from odw.core.etl.transformation.curated.curation_process import CurationProcess


class LegacyFolderDataCurationProcess(CurationProcess):
    HARMONISED_TABLE = "odw_harmonised_db.horizon_folder"
    OUTPUT_TABLE = "odw_curated_db.legacy_folder_data"

    @classmethod
    def get_name(cls) -> str:
        return "Legacy Folder Data Curation Process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("LegacyFolderDataCurationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("LegacyFolderDataCurationProcess.process() has not been implemented yet.")

