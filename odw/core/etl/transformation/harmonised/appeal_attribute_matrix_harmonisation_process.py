from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from typing import Any


class AppealAttributeMatrixHarmonisationProcess(HarmonisationProcess):
    STANDARDISED_TABLE = "appeal_attribute_matrix"
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "Appeal Attribute Matrix Harmonisation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealAttributeMatrixHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealAttributeMatrixHarmonisationProcess.process() has not been implemented yet.")
