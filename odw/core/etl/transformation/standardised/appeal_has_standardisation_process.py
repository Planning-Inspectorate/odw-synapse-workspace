from typing import Any
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class AppealHasStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "horizon_appeal_has"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "appeal_has_standardisation_process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasStandardisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasStandardisationProcess.process() has not been implemented yet.")