from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealServiceUserCuratedMipinsProcess(CurationProcess):
    SOURCE_SB_SERVICE_USER_TABLE = "sb_service_user"
    SOURCE_SERVICE_USER_TABLE = "service_user"
    OUTPUT_TABLE = "appeal_service_user_curated_mipins"

    def get_name(self) -> str:
        return "appeal_service_user_curated_mipins_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealServiceUserCuratedMipinsProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealServiceUserCuratedMipinsProcess.process() has not been implemented yet.")