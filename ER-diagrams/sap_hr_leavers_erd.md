```mermaid
erDiagram
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" {
        string PersNo PK
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
        date OrgStartDate
        date Leaving PK
        string Act
        string ActionType
        string ActR
        string ReasonforAction
        string S
        string EmploymentStatus
        string EmployeeNo
        string Position
        string Position1
        double Annualsalary
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
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|STAGE_SAP_HR_LEAVERS" {
        string PersNo PK
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
        date OrgStartDate
        date Leaving PK
        string Act
        string ActionType
        string ActR
        string ReasonforAction
        string S
        string EmploymentStatus
        string EmployeeNo
        string Position
        string Position1
        double Annualsalary
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
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_standardised_db|SAP_HR_LEAVERS_MONTHLY" {
        string PersNo PK
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
        string OrgStartDate
        string Leaving
        string Act
        string ActionType
        string ActR
        string ReasonforAction
        string S
        string EmploymentStatus
        string EmployeeNo
        string Position
        string Position1
        string Annualsalary
        string Curr
        string UserID
        string EmailAddress
        string PersNo1
        string NameofManagerOM
        string ManagerPosition
        string ManagerPositionText
        string LMEmail
    }

	"odw_harmonised_db|HIST_SAP_HR" {
        string PersNo PK
        date Report_MonthEnd_Date PK
        date OrgStartDate
        decimal FTE2
        string CostCtr
        string PA
        string PSubarea
        string Orgunit
        string PSgroup
        string WorkC
        string WorkContract
        string GenderKey
        date Birthdate
        decimal Wkhrs
        decimal leave_entitlement_hrs
        decimal leave_taken_hrs
        decimal leave_remaining_hours
        decimal leave_remaining_prorata_hours
        decimal Annualsalary
        string Position1
        date FixTermEndDate
        string ContractType
        string PArea
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

	"odw_curated_db|VW_HR_MEASURES" {
        string PersonReportMonthEndKey PK
        string PersonKey FK
        decimal FTE
        string dim_cost_centre_key
        int dim_personnel_area_key
        int dim_personnel_subarea_key
        int dim_organisation_key
        string Grade
        int dim_work_contract_key
        int dim_gender_key
        int dim_date_key FK
        string ReportFlag
        int dim_started_date_key FK
        int dim_leaving_date_key FK
        string ReasonForAction
        string PArea
        decimal WorkingHours
        decimal LengthOfService
        decimal Age
        decimal LeaveEntitlementHours
        decimal LeaveTakenHours
        decimal LeaveRemainingHours
        decimal LeaveRemainingProrataHours
        decimal AnnualSalary
        string Position1
        string Fix_Term_End_Date
        string Contract_Type
        string Directorate
        int LOS_SortKey
        string Age_Group_Civil_Service
        decimal Hourly_Rate
        string ReasonforAction_Voluntary_Involuntary
        decimal Leave_Remaining_Pro_rata_Cost
        string Age_Group_MHCLG
        decimal LeaveEntitlementDays
        string PartTimeFullTime
        string Country
    }
    
    "odw_standardised_db|SAP_HR_LEAVERS_MONTHLY" ||--|| "odw_harmonised_db|STAGE_SAP_HR_LEAVERS" : "transforms_to"
    "odw_harmonised_db|STAGE_SAP_HR_LEAVERS" ||--|| "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" : "loads_to"
	"odw_harmonised_db|HIST_SAP_HR" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_person_no_data"
	"odw_harmonised_db|LIVE_DIM_DATE" ||--o{ "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" : "provides_leavers_joiners_date"
    "odw_harmonised_db|LIVE_DIM_DATE_LEAVERS" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_leavers_date"
    "odw_harmonised_db|LIVE_DIM_DATE_STARTED" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_started_date"
	"odw_harmonised_db|LOAD_SAP_HR_LEAVERS" ||--o{ "odw_curated_db|VW_HR_MEASURES" : "provides_leaver_data"
