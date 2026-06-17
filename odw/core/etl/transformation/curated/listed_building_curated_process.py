from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess


class ListedBuildingCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "listed_building"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "Listed Building Curation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("ListedBuildingCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("ListedBuildingCuratedProcess.process() has not been implemented yet.")
