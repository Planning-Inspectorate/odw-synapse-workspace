from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from typing import Any


class AppealAttributeMatrixStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_attribute_matrix_standardisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealAttributeMatrixStandardisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealAttributeMatrixStandardisationProcess.process() has not been implemented yet.")
