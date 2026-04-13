from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess

class NsipProjectHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "nsip_project"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "nsip_project_harmonisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError(
            "NsipProjectHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "NsipProjectHarmonisationProcess.process() has not been implemented yet."
        )