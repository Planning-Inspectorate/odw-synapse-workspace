from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class AppealS78StandardisationProcess(StandardisationProcess):
    STANDARDISED_DB = "odw_standardised_db"
    STANDARDISED_CASES_SPECIALISMS = "cases_specialisms"
    STANDARDISED_HORIZON_CASES_S78 = "horizoncases_s78"
    STANDARDISED_HORIZON_CASE_DATES = "vw_case_dates"
    OUTPUT_TABLE = "horizon_appeal_s78"

    def get_name(self) -> str:
        return "Appeal S78 Standardisation Process"
    
    def _load_standardised_horizoncases_s78(self):
        pass

    def _load_standardised_cases_specialisms(self):
        pass

    def _load_standardised_vw_case_dates(self):
        pass

    def _load_standardised_casedocumentdatesdates(self):
        pass

    def _load_standardised_casesitestrings(self):
        pass

    def _load_standardised_typeofprocedure(self):
        pass

    def _load_standardised_vw_addadditionaldata(self):
        pass

    def _load_standardised_vw_additionalfields(self):
        pass

    def _load_standardised_horizon_advert_attributes(self):
        pass

    def _load_standardised_TypeOfLevel(self):
        pass

    def _load_standardised_horizon_specialist_case_dates(self):
        pass

    def _load_standardised_PlanningAppStrings(self):
        pass

    def _load_standardised_PlanningAppDates(self):
        pass

    def _load_standardised_BIS_LeadCase(self):
        pass

    def _load_standardised_CaseStrings(self):
        pass

    def _load_standardised_horizon_case_info(self):
        pass

    def _load_standardised_horizon_case_dates(self):
        pass

    def _load_standardised_horizon_appeals_additional_data(self):
        pass

    def _load_standardised_horizon_appeal_grounds(self):
        pass

    def _load_standardised_horizon_notice_dates(self):
        pass

    def _load_standardised_horizon_application_made_under_section(self):
        pass


    def load_data(self, **kwargs):
        raise NotImplementedError("AppealS78StandardisationProcess.load_data() has not been implemented yet.")

    def process(self, **kwargs):
        raise NotImplementedError("AppealS78StandardisationProcess.process() has not been implemented yet.")
