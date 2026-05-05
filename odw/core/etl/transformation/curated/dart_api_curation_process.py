from odw.core.etl.transformation.curated.curation_process import CurationProcess
from pyspark.sql import DataFrame
from typing import Any, Dict

class DartAPICurationProcess(CurationProcess):
    HARMONISED_APPEAL_HAS = "appeal_has"
    HARMONISED_PINS_LPA = "pins_lpa"
    HARMONISED_SB_SERVICE_USER = "sb_service_user"
    HARMONISED_SB_APPEAL_EVENT = "sb_appeal_event"
    HARMONISED_PINS_INSPECTOR = "horizon_pins_inspector"
    HARMONISED_ENTRAID = "entraid"
    HARMONISED_APPEAL_S78 = "horizon_appeal_s78"
    OUTPUT_TABLE = "dart_api"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "dart_api_curation_process"
    
    def _load_harmonised_appeal_has(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_appeal_has() has not been implemented yet.")
    
    def _load_harmonised_pins_lpa(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_pins_lpa() has not been implemented yet.")
    
    def _load_harmonised_sb_service_user(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_sb_service_user() has not been implemented yet.")

    def _load_harmonised_sb_appeal_event(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_sb_appeal_event() has not been implemented yet.")
    
    def _load_harmonised_entraid(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_entraid() has not been implemented yet.")
    
    def _load_harmonised_horizon_pins_inspector(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_horizon_pins_inspector() has not been implemented yet.")
    
    def _load_harmonised_appeal_s78(self):
        raise NotImplementedError("DartAPICurationProcess._load_harmonised_appeal_s78() has not been implemented yet.")

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("DartAPICurationProcess.load_data() has not been implemented yet.")
    
    def _aggregate_appeal_has_data(self, appeal_has_data: DataFrame, pins_lpa: DataFrame, sb_service_user: DataFrame, sb_appeal_event: DataFrame, entraid: DataFrame, horizon_pins_inspector: DataFrame):
        raise NotImplementedError("DartAPICurationProcess._aggregate_appeal_has_data() has not been implemented yet.")
    
    def _aggregate_appeal_s78_data(self, appeal_s78_data: DataFrame, pins_lpa: DataFrame, sb_service_user: DataFrame, sb_appeal_event: DataFrame, entraid: DataFrame, horizon_pins_inspector: DataFrame):
        raise NotImplementedError("DartAPICurationProcess._aggregate_appeal_s78_data() has not been implemented yet.")
    
    def _union_has_and_s78_data(self, appeal_has_data: DataFrame, appeal_s78_data: DataFrame) -> DataFrame:
        raise NotImplementedError("DartAPICurationProcess._union_has_and_s78_data() has not been implemented yet.")
    
    def _clean_aggregate_data(self, aggregate_data: DataFrame) -> DataFrame:
        raise NotImplementedError("DartAPICurationProcess._clean_aggregate_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs) -> Dict[str, DataFrame]:
        raise NotImplementedError("DartAPICurationProcess.process() has not been implemented yet.")
