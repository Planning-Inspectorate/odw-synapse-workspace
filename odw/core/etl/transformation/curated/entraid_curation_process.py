from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLResult
from pyspark.sql import DataFrame
from typing import Tuple, Dict
from typing import Any


class EntraIDCurationProcess(CurationProcess):
    HARMONISED_TABLE = "entraid"
    CURATED_TABLE = "entraid"

    def get_name(self) -> str:
        return "Entrad ID Curation"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("EntraIDCurationProcess.load_data() has not been implemented yet.")

    def _clean_data(self, raw_entraid_data: DataFrame):
        raise NotImplementedError("EntraIDCurationProcess._clean_data() has not been implemented yet.")

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        raise NotImplementedError("EntraIDCurationProcess.process() has not been implemented yet.")
