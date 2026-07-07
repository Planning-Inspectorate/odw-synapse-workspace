from typing import Any, Dict
from odw.core.etl.transformation.standardised.standardisation_process import (
    StandardisationProcess,
)
from pyspark.sql import DataFrame


class ListedBuildingStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "listed_building"

    @classmethod
    def get_name(cls) -> str:
        return "Listed Building Standardisation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError(
            "ListedBuildingStandardisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "ListedBuildingStandardisationProcess.process() has not been implemented yet."
        )
