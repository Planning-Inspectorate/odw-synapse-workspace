from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class AppealS78StandardisationProcess(StandardisationProcess):
    STANDARDISED_DB = "odw_standardised_db"
    STANDARDISED_CASES_SPECIALISMS = "cases_specialisms"
    STANDARDISED_HORIZON_CASES_S78 = "horizoncases_s78"
    STANDARDISED_HORIZON_CASE_DATES = "vw_case_dates"
    STANDARDISED_CASE_DOCUMENT_DATES_DATES = "casedocumentdatesdates"
    STANDARDISED_CASE_SITE_STRINGS = "casesitestrings"
    STANDARDISED_ADDITIONAL_FIELDS = "vw_additionalfields"
    STANDARDISED_HORIZON_ADVERT_ATTRIBUTES = "horizon_advert_attributes"
    STANDARDISED_HORIZON_SPECIALIST_CASE_DATES = "horizon_specialist_case_dates"
    STANDARDISED_PLANNING_APP_STRINGS = "PlanningAppStrings"
    STANDARDISED_PLANNING_APP_DATES = "PlanningAppDates"
    STANDARDISED_LEAD_CASE = "BIS_LeadCase"
    OUTPUT_TABLE = "horizon_appeal_s78"
    STANDARDISED_CASE_STRINGS = "CaseStrings"
    STANDARDISED_HORIZON_CASE_INFO = "horizon_case_info"
    STANDARDISED_HORIZON_CASE_DATES = "horizon_case_dates"
    STANDARDISED_HORIZON_APPEALS_ADDITIONAL_DATA = "horizon_appeals_additional_data"
    STANDARDISED_HORIZON_NOTICE_DATES = "horizon_notice_dates"
    STANDARDISED_HORIZON_APPLICATION_MADE_UNDER_SECTION = "horizon_application_made_under_section"
    STANDARDISED_ADD_ADDITIONAL_DATA = "vw_addadditionaldata"
    STANDARDISED_HORIZON_APPEAL_GROUNDS = "horizon_appeal_grounds"
    STANDARDISED_TYPE_OF_PROCEDURE = "typeofprocedure"
    STANDARDISED_TYPE_OF_LEVEL = "TypeOfLevel"

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

    def _load_type_of_procedure(self):
        pass

    def _load_standardised_vw_addadditionaldata(self):
        pass

    def _load_standardised_vw_additionalfields(self):
        pass

    def _load_standardised_horizon_advert_attributes(self):
        pass

    def _load_standardised_current_type_of_level(self):
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
