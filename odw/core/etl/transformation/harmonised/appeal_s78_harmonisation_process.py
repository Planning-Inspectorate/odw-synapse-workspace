from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class AppealS78HarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "appeal_s78"

    def get_name(self) -> str:
        return "Appeal S78 Harmonisation Process"

    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78HarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78HarmonisationProcess.process() has not been implemented yet.")
