from odw.core.etl.transformation.curated.curation_process import CurationProcess


class AppealS78CurationProcess(CurationProcess):
    OUTPUT_TABLE = "appeal_s78"

    def get_name(self) -> str:
        return "Appeal S78 Curation Process"

    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78CurationProcess.load_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78CurationProcess.process() has not been implemented yet.")
