```mermaid
erDiagram
    "odw_harmonised_db|HIST_SAP_HR" {
        string PersNo PK
        date Report_MonthEnd_Date PK
		string Firstname
		string Lastname
		string EmployeeNo
		string CoCd
		string CompanyCode
		string PA
		string PersonnelArea
		string PSubarea
		string PersonnelSubarea
		string Orgunit
		string OrganizationalUnit
		string Organizationalkey
		string OrganizationalKey1
		string WorkC
		string WorkContract
		string CT
		string ContractType
		string PSgroup
		string PayBandDescription
		float FTE
		float Wkhrs
		string IndicatorPartTimeEmployee
		string S
		string EmploymentStatus
		string GenderKey
		string TRAStartDate
		string TRAEndDate
		string TRAStatus
		string TRAGrade
		string PrevPersNo
		string ActR
		string ReasonforAction
		string Position
		string Position1
		string CostCtr
		string CostCentre
		date CivilServiceStart
		varchar DatetoCurrentJob
		varchar SeniorityDate
		date DatetoSubstGrade
		string PersNo1
		string NameofManagerOM
		string ManagerPosition
		string ManagerPositionText
		string CounterSignManager
		string Loc
		string Location
		date OrgStartDate
		string FixTermEndDate
		string LoanStartDate
		string LoanEndDate
		string EEGrp
		string EmployeeGroup
		string Annualsalary
		string Curr
		string NInumber
		string Birthdate
		string Ageofemployee
		string EO
		string Ethnicorigin
		string NID
		string Rel
		string ReligiousDenominationKey
		string SxO
		string WageType
		string EmployeeSubgroup
		string LOAAbsType
		string LOAAbsenceTypeText
		string Schemereference
		string PensionSchemeName
		string DisabilityCode
		string DisabilityText
		string DisabilityCodeDescription
		string PArea
		string PayrollArea
		string AssignmentNumber
		float FTE2
		date PDAC_ETL_Date
		varchar SourceSystemID
		date IngestionDate
		date ValidTo
		string RowID
		string IsActive
		float leave_entitlement_hrs
		float leave_taken_hrs
		float leave_remaining_hours
		float leave_remaining_prorata_hours
    }
    
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" {
        string PersNo PK
        date OrgStartDate PK
        string Lastname
        string Firstname
        string CoCd
        string CompanyCode
        string Loc
        string Location
        string PSgroup
        string PayBandDescription
        string Orgunit
        string OrganizationalUnit
        string PA
        string PersonnelArea
        string PSubarea
        string PersonnelSubarea
        string WorkC
        string WorkContract
        date Leaving
        string Act
        string ActionType
        string ActR
        string ReasonforAction
        string S
        string EmploymentStatus
        string EmployeeNo
        string Position
        string Position1
        float Annualsalary
        string Curr
        string UserID
        string EmailAddress
        string PersNo1
        string NameofManagerOM
        string ManagerPosition PK
        string ManagerPositionText
        string LMEmail
        string SourceSystemID
        date IngestionDate
        date ValidTo
        string RowID
        string IsActive		
    }
    
    "odw_standardised_db|SAP_FINANCE_DIM_COSTCENTRE" {
        string CostCentre PK
        string CostCentreDesc
        string Branch
        string Directorate
        string current_record
		date ingested_datetime
		date expected_from
		date expected_to
		string ID
		string cc_level_1
		string cc_level_2
		string cc_level_3
		string cc_level_4
		string dim_check_sum
		string effective_date
		string end_date
		string last_updated
    }
    
    "odw_harmonised_db|LIVE_DIM_DATE" {
        date date PK
        int dim_date_key
		int	day_int
		string day_name
		string month_name
        int month_int
        int year_int
        date first_of_month
		int week_int
		int iso_week_int
		int day_of_week_int
		int quarter_int
		string quarter_name
		int year_int
		string first_of_year
		string week_ending_date
		string financial_year
		int FY_yyyy
		int week_ending_year
		int	week_ending_quarter_int
		string week_ending_quarter_name
		int week_ending_month_int
		string week_ending_month_name
		int week_ending_day
		int	MonthYearSortKey
		string MonthYear
		string FY
		string FY_Latest
		string FY_MonthYearLatest
		string FY_Quarter
		int FY_Latest_SortKey
		string week_starting_date		
    }
	
	"odw_harmonised_db|LIVE_DIM_DATE_LEAVERS" {
        date date PK
        int dim_date_key
		int	day_int
		string day_name
		string month_name
        int month_int
        int year_int
        date first_of_month
		int week_int
		int iso_week_int
		int day_of_week_int
		int quarter_int
		string quarter_name
		int year_int
		string first_of_year
		string week_ending_date
		string financial_year
		int FY_yyyy
		int week_ending_year
		int	week_ending_quarter_int
		string week_ending_quarter_name
		int week_ending_month_int
		string week_ending_month_name
		int week_ending_day
		int	MonthYearSortKey
		string MonthYear
		string FY
		string FY_Latest
		string FY_MonthYearLatest
		string FY_Quarter
		int FY_Latest_SortKey
		string week_starting_date
    }

	"odw_harmonised_db|LIVE_DIM_DATE_STARTED" {
        date date PK
        int dim_date_key
		int	day_int
		string day_name
		string month_name
        int month_int
        int year_int
        date first_of_month
		int week_int
		int iso_week_int
		int day_of_week_int
		int quarter_int
		string quarter_name
		int year_int
		string first_of_year
		string week_ending_date
		string financial_year
		int FY_yyyy
		int week_ending_year
		int	week_ending_quarter_int
		string week_ending_quarter_name
		int week_ending_month_int
		string week_ending_month_name
		int week_ending_day
		int	MonthYearSortKey
		string MonthYear
		string FY
		string FY_Latest
		string FY_MonthYearLatest
		string FY_Quarter
		int FY_Latest_SortKey
		string week_starting_date
    }
    
    "odw_curated_db|VW_DIM_HR_PERSONNEL_AREA" {
        int dim_personnel_area_key PK
        string PersonnelAreaCode
        string PersonnelArea
    }
    
    "odw_curated_db|VW_DIM_HR_PERSONNEL_SUBAREA" {
        int dim_personnel_subarea_key PK
        string PersonnelSubAreaCode
        string PersonnelSubArea
    }
    
    "odw_curated_db|VW_DIM_HR_ORGANISATION_UNIT" {
        int dim_organisation_key PK
        string OrganisationUnitCode
        string OrganisationUnitDesc
    }
    
    "odw_curated_db|PBI_DIM_HR_GRADE" {
        string Grade PK
        string GradeDescription
		int	dim_grade_key
		string Inspector_Support
		string GradeGroup
		int SortKey

    }
    
    "odw_curated_db|VW_DIM_HR_WORK_CONTRACT" {
        int dim_work_contract_key PK
        string WorkContractCode
        string WorkContract
    }
    
    "odw_curated_db|VW_DIM_HR_GENDER" {
        int dim_gender_key PK
        string gender
    }
    
    "odw_curated_db|VW_HR_MEASURES" {
        string PersonReportMonthEndKey PK
        string PersonKey
        float FTE
        string dim_cost_centre_key FK
        int dim_personnel_area_key FK
        int dim_personnel_subarea_key FK
        int dim_organisation_key FK
        string Grade FK
        int dim_work_contract_key FK
        int dim_gender_key FK
        int dim_date_key FK
        string ReportFlag
        int dim_started_date_key FK
        int dim_leaving_date_key FK
        string ReasonForAction
        string PArea
        float WorkingHours
        float LengthOfService
        float Age
        float LeaveEntitlementHours
        float LeaveTakenHours
        float LeaveRemainingHours
        float LeaveRemainingProrataHours
        float AnnualSalary
        string Position1
        string Fix_Term_End_Date
        string Contract_Type
        string Directorate
        int LOS_SortKey
        string Age_Group_Civil_Service
        float Hourly_Rate
        string ReasonforAction_Voluntary_Involuntary
        float Leave_Remaining_Pro_rata_Cost
        string Age_Group_MHCLG
        float LeaveEntitlementDays
        string PartTimeFullTime
        string Country
    }
    
    "odw_curated_db|PBI_HR_MEASURES" {
        string PersonReportMonthEndKey PK
        string PersonKey
        float FTE
        string dim_cost_centre_key
        int dim_personnel_area_key
        int dim_personnel_subarea_key
        int dim_organisation_key
        string Grade
        int dim_work_contract_key
        int dim_gender_key
        int dim_date_key
        string ReportFlag
        int dim_started_date_key
        int dim_leaving_date_key
        string ReasonForAction
        string PArea
        float WorkingHours
        float LengthOfService
        float Age
        float LeaveEntitlementHours
        float LeaveTakenHours
        float LeaveRemainingHours
        float LeaveRemainingProrataHours
        float AnnualSalary
        string Position1
        string Fix_Term_End_Date
        string Contract_Type
        string Directorate
        int LOS_SortKey
        string Age_Group_Civil_Service
        float Hourly_Rate
        string ReasonforAction_Voluntary_Involuntary
        float Leave_Remaining_Pro_rata_Cost
        string Age_Group_MHCLG
        float LeaveEntitlementDays
        string PartTimeFullTime
        string Country
    }
    
    "odw_harmonised_db|HIST_SAP_HR" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_employee_data"
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_leavers_data"
    "odw_standardised_db|SAP_FINANCE_DIM_COSTCENTRE" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "cost_centre_lookup"
    "odw_harmonised_db|LIVE_DIM_DATE" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "date_dimension_lookup"
	"odw_harmonised_db|LIVE_DIM_DATE_STARTED" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "date_started_lookup"
	"odw_harmonised_db|LIVE_DIM_DATE_LEAVERS" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "date_leavers_lookup"
    "odw_curated_db|VW_DIM_HR_PERSONNEL_AREA" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "personnel_area_lookup"
    "odw_curated_db|VW_DIM_HR_PERSONNEL_SUBAREA" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "personnel_subarea_lookup"
    "odw_curated_db|VW_DIM_HR_ORGANISATION_UNIT" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "organisation_unit_lookup"
    "odw_curated_db|PBI_DIM_HR_GRADE" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "grade_lookup"
    "odw_curated_db|VW_DIM_HR_WORK_CONTRACT" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "work_contract_lookup"
    "odw_curated_db|VW_DIM_HR_GENDER" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "gender_lookup"
    "odw_curated_db|VW_HR_MEASURES" ||--|| "odw_curated_db|PBI_HR_MEASURES" : "curates_to"
