from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealS78MIPINSCurationProcess(CurationProcess):
    OUTPUT_TABLE = "appeals_s78_curated_mipins"

    def get_name(self) -> str:
        return "Appeal S78 MIPINS Curation Process"

    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78MIPINSCurationProcess.load_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78MIPINSCurationProcess.process() has not been implemented yet.")
