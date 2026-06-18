from typing import Any, Dict
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from pyspark.sql import DataFrame


class AppealHasHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "appeal_has"
    SERVICE_BUS_TABLE = "sb_appeal_has"
    HORIZON_TABLE = "horizon_appeal_has"
    GROUP_RESOLVER_TABLE = "GroupResolver"
    S78_TABLE = "appeal_s78"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal HAS Harmonisation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("AppealHasHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasHarmonisationProcess.process() has not been implemented yet.")
