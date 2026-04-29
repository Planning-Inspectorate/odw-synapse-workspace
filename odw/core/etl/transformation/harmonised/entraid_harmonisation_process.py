from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class EntraIdHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "entraid"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    @classmethod
    def get_name(cls) -> str:
        return "EntraID Harmonisation"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("EntraIdHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("EntraIdHarmonisationProcess.process() has not been implemented yet.")
