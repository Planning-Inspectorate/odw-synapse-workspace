from typing import Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class ListedBuildingHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "listed_building"

    @classmethod
    def get_name(cls) -> str:
        return "Listed Building Harmonisation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("ListedBuildingHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("ListedBuildingHarmonisationProcess.process() has not been implemented yet.")
