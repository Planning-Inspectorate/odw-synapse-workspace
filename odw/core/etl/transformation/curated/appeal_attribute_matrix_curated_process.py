from odw.core.etl.transformation.curated.curation_process import CurationProcess
from typing import Any


class AppealAttributeMatrixCuratedProcess(CurationProcess):
    STANDARDISED_TABLE = "appeal_attribute_matrix"
    HARMONISED_TABLE = "ref_appeal_attribute_matrix"
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "Appeal Attribute Matrix Curation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealAttributeMatrixCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealAttributeMatrixCuratedProcess.process() has not been implemented yet.")
