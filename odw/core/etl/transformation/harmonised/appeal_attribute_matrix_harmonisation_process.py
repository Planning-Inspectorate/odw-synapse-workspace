from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from typing import Any


class AppealAttributeMatrixHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_attribute_matrix_harmonisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.process() has not been implemented yet."
        )
