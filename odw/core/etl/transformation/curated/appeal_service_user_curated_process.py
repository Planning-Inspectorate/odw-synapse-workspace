from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealServiceUserCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_service_user"
    SOURCE_TABLE = "service_user"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_service_user_curated_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealServiceUserCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealServiceUserCuratedProcess.process() has not been implemented yet.")
