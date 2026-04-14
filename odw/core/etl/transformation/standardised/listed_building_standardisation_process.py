from typing import Any
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class ListedBuildingStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "listed_building"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "listed_building_standardisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("ListedBuildingStandardisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("ListedBuildingStandardisationProcess.process() has not been implemented yet.")