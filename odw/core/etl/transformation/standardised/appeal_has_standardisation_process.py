from typing import Any
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess


class AppealHasStandardisationProcess(StandardisationProcess):
    OUTPUT_TABLE = "horizon_appeal_has"
    STANDARDISED_DB = "odw_standardised_db"
    STANDARDISED_CASES_SPECIALISMS = "cases_specialisms"
    STANDARDISED_HORIZON_CASES_HAS = "horizoncases_has"
    STANDARDISED_VW_CASE_DATES = "vw_case_dates"
    STANDARDISED_CASE_DOCUMENT_DATES_DATES = "casedocumentdatesdates"
    STANDARDISED_CASE_SITE_STRINGS = "casesitestrings"
    STANDARDISED_ADDITIONAL_FIELDS = "vw_additionalfields"
    STANDARDISED_HORIZON_ADVERT_ATTRIBUTES = "horizon_advert_attributes"
    STANDARDISED_HORIZON_SPECIALIST_CASE_DATES = "horizon_specialist_case_dates"
    STANDARDISED_PLANNING_APP_STRINGS = "PlanningAppStrings"
    STANDARDISED_PLANNING_APP_DATES = "PlanningAppDates"
    STANDARDISED_LEAD_CASE = "BIS_LeadCase"
    STANDARDISED_CASE_STRINGS = "CaseStrings"
    STANDARDISED_HORIZON_CASE_INFO = "horizon_case_info"
    STANDARDISED_HORIZON_CASE_DATES = "horizon_case_dates"
    STANDARDISED_HORIZON_APPEALS_ADDITIONAL_DATA = "horizon_appeals_additional_data"
    STANDARDISED_ADD_ADDITIONAL_DATA = "vw_addadditionaldata"
    STANDARDISED_TYPE_OF_PROCEDURE = "typeofprocedure"
    STANDARDISED_TYPE_OF_LEVEL = "TypeOfLevel"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "Appeal HAS Standardisation Process"

    def load_data(self) -> dict[str, Any]:
        raise NotImplementedError("AppealHasStandardisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError("AppealHasStandardisationProcess.process() has not been implemented yet.")
