from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class AppealEventHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "appeal_event"
    SERVICE_BUS_TABLE = "sb_appeal_event"
    HORIZON_TABLE = "horizon_appeals_event"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "Appeal Event Harmonisation Process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("AppealEventHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("AppealEventHarmonisationProcess.process() has not been implemented yet.")
