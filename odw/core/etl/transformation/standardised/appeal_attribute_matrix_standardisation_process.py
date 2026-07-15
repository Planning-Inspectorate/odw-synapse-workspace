from odw.core.etl.transformation.standardised.standardisation_process import (
    StandardisationProcess,
)
from typing import Any, Dict
from pyspark.sql import DataFrame


class AppealAttributeMatrixStandardisationProcess(StandardisationProcess):
    CSV_FOLDER = "AppealAttributeMatrix"
    OUTPUT_TABLE = "appeal_attribute_matrix"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal Attribute Matrix Standardisation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError(
            "AppealAttributeMatrixStandardisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealAttributeMatrixStandardisationProcess.process() has not been implemented yet."
        )
