from typing import Any

from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealEventCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_event"
    SOURCE_TABLE = "appeal_event"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "Appeal Event Curation Process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealEventCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealEventCuratedProcess.process() has not been implemented yet.")
