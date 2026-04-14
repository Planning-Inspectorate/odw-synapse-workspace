from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class ListedBuildingCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "listed_building"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "listed_building_curated_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("ListedBuildingCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("ListedBuildingCuratedProcess.process() has not been implemented yet.")
