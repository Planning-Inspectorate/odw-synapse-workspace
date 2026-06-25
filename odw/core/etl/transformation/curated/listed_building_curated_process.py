from typing import Any, Dict
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from pyspark.sql import DataFrame


class ListedBuildingCuratedProcess(CurationProcess):
    OUTPUT_TABLE = "listed_building"

    @classmethod
    def get_name(cls) -> str:
        return "Listed Building Curation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("ListedBuildingCuratedProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("ListedBuildingCuratedProcess.process() has not been implemented yet.")
