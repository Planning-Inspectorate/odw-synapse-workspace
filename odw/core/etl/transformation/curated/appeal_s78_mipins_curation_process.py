from odw.core.etl.transformation.curated.curation_process import CurationProcess
from pyspark.sql import DataFrame


class AppealS78MIPINSCurationProcess(CurationProcess):
    APPEALS_S78_TABLE = "appeal_s78"
    OUTPUT_TABLE = "appeals_s78_curated_mipins"
    @classmethod
    def get_name(cls) -> str:
        return "Appeal S78 MIPINS Curation Process"

    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78MIPINSCurationProcess.load_data() has not been implemented yet.")

    def _filter_data(self, appeals_s78_data: DataFrame) -> DataFrame:
        raise NotImplementedError("AppealS78MIPINSCurationProcess._filter_data() has not been implemented yet.")

    def _clean_data(self, appeals_s78_data: DataFrame) -> DataFrame:
        raise NotImplementedError("AppealS78MIPINSCurationProcess._clean_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78MIPINSCurationProcess.process() has not been implemented yet.")
