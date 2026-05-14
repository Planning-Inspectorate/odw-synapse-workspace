from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLResult
from pyspark.sql import DataFrame
from typing import Tuple, Dict
from typing import Any


class EntraIDMIPINSCurationProcess(CurationProcess):
    HARMONISED_TABLE = "entraid"
    CURATED_TABLE = "entraid_curated_mipins"

    def get_name(self) -> str:
        return "Entrad ID Curation"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("EntraIDMIPINSCurationProcess.load_data() has not been implemented yet.")

    def _clean_data(self, raw_entraid_data: DataFrame):
        raise NotImplementedError("EntraIDMIPINSCurationProcess._clean_data() has not been implemented yet.")

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        raise NotImplementedError("EntraIDMIPINSCurationProcess.process() has not been implemented yet.")
