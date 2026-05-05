from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class AppealHasHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "appeal_has"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_has_harmonisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasHarmonisationProcess.process() has not been implemented yet.")