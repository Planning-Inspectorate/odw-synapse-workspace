from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class AppealS78StandardisationProcess(StandardisationProcess):
    STANDARDISED_DB = "odw_standardised_db"
    STANDARDISED_CASES_SPECIALISMS = "cases_specialisms"
    OUTPUT_TABLE = "horizon_appeal_s78"

    def get_name(self) -> str:
        return "Appeal S78 Standardisation Process"

    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78StandardisationProcess.load_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78StandardisationProcess.process() has not been implemented yet.")
