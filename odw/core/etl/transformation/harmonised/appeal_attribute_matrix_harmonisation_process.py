from odw.core.etl.transformation.harmonised.harmonisation_process import (
    HarmonisationProcess,
)
from typing import Any, Dict
from pyspark.sql import DataFrame


class AppealAttributeMatrixHarmonisationProcess(HarmonisationProcess):
    STANDARDISED_TABLE = "appeal_attribute_matrix"
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal Attribute Matrix Harmonisation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.process() has not been implemented yet."
        )
