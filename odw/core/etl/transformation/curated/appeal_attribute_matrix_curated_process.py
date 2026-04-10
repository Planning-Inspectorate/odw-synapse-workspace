from odw.core.etl.transformation.curated.curation_process import CurationProcess
from typing import Any


class AppealAttributeMatrixCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_attribute_matrix_curated_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealAttributeMatrixCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealAttributeMatrixCuratedProcess.process() has not been implemented yet.")
