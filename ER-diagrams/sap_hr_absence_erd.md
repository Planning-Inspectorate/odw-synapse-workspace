```mermaid
erDiagram
    "odw_standardised_db|HR_ABSENCE_MONTHLY" {
        string StaffNumber PK
        string AbsType PK
        string SicknessGroup
        string StartDate PK
        string EndDate
        string AttendanceorAbsenceType
        string Days
        string Hrs
        string Caldays
        string WorkScheduleRule
        string Wkhrs
        string HrsDay
        string WkDys
		string Start
		string Endtime		
        string AnnualLeaveStart
		string Illness
		date ingested_datetime
		date expected_from
		date expected_to		
    }
    
    "odw_harmonised_db|SAP_HR_ABSENCE_ALL" {
        string StaffNumber PK
        string AbsType PK
        string SicknessGroup
        date StartDate PK
        date EndDate
        string AttendanceorAbsenceType
        double Days
        double Hrs
        date Start
        date Endtime
        string Caldays
        string WorkScheduleRule
        double Wkhrs
        double HrsDay
        double WkDys
        date AnnualLeaveStart
        string SourceSystemID
        date IngestionDate
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL_TEMP" {
        date absencedate PK
        decimal absencehours
        string staffnumber PK
        string WorkScheduleRule
        string AbsType
        string SicknessGroup
        string AttendanceorAbsenceType
        decimal Leave
        string PSGroup
        string PersonnelArea
        string PersonnelSubarea
        string SourceSystemID
        date IngestionDate
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL" {
        date absencedate PK
        decimal absencehours
        string staffnumber PK
        string WorkScheduleRule
        string AbsType
        string SicknessGroup
        string AttendanceorAbsenceType
        decimal Leave
        string PSGroup
        string PersonnelArea
        string PersonnelSubarea
        string SourceSystemID
        date IngestionDate
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_SICKNESS" {
        int sickness_id PK
        string StaffNumber
        double Days
        date sickness_start
        date sickness_end
        string FY
        string financial_year
        string calendar_year
        string SourceSystemID
        date IngestionDate
        timestamp ValidFrom
        timestamp ValidTo
        string RowID
        string IsActive
        timestamp LastUpdated
    }
    
    "odw_harmonised_db|SAP_HR_WORKSCHEDULERULE" {
        string WorkScheduleRule PK
		string WorkScheduleCode
        string MoWk1
        string TuWk1
        string WeWk1
        string ThWk1
        string FrWk1
        string MoWk2
        string TuWk2
        string WeWk2
        string ThWk2
        string FrWk2
		float WkHrsWk1
		float WkHrsWk2
		float AvgWkHrs
		date WSRstart
		string Currentweek
		date Lastmodified
		date IngestionDate
    }
    
    "odw_standardised_db|WORK_SCHEDULES" {
        string WorkScheduleRule PK
        string Mo
        string Tu
        string We
        string Th
        string Fr
		string WkHrs
		string MoTuWeThFr		
		date ingested_datetime
		date expected_from
		date expected_to		
    }
    
    "odw_harmonised_db|HIST_SAP_HR" {
        string PersNo PK
        date Report_MonthEnd_Date PK
        string PSGroup
        string PersonnelArea
        string PersonnelSubarea
		string Firstname
		string Lastname
		string EmployeeNo
		string CoCd
		string CompanyCode
		string PA
		string PSubarea
		string Orgunit
		string OrganizationalUnit
		string Organizationalkey
		string OrganizationalKey1
		string WorkC
		string WorkContract
		string CT
		string ContractType
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
    
    "odw_standardised_db|LIVE_HOLIDAYS" {
        string HolidayDate PK
		string HolidayDescription		
		date ingested_datetime
		date expected_from
		date expected_to
		
    }
    
    "odw_harmonised_db|LIVE_DIM_DATE" {
        date date PK
        int dim_date_key
        string FY
        string financial_year
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
		int FY_yyyy
		int week_ending_year
		int	week_ending_quarter_int
		string week_ending_quarter_name
		int week_ending_month_int
		string week_ending_month_name
		int week_ending_day
		int	MonthYearSortKey
		string MonthYear
		string FY_Latest
		string FY_MonthYearLatest
		string FY_Quarter
		int FY_Latest_SortKey
		string week_starting_date		
    }
    
    "odw_curated_db|VW_FACT_ABSENCE_ALL" {
		date	absencedate PK
		float	absencehours
		string	staffnumber PK
		string	WorkScheduleRule
		string	AbsType
		string	SicknessGroup
		string	AttendanceorAbsenceType
		float	Leave
		float	Leave_ONS
		string	PSGroup
		string	PersonnelArea
		string	PersonnelSubarea
		int	sickness_id
		float	sicknesslength
		string	sicknesslengthtype
		
    }
    
    "odw_curated_db|PBI_FACT_ABSENCE_ALL" {
        date absencedate PK
        float absencehours
        string staffnumber PK
        string WorkScheduleRule
        string AbsType
        string SicknessGroup
        string AttendanceorAbsenceType
        float Leave
        float Leave_ONS
        string PSGroup
        string PersonnelArea
        string PersonnelSubarea
        int sickness_id
        float sicknesslength
        string sicknesslengthtype
    }
    
    "odw_standardised_db|HR_ABSENCE_MONTHLY" ||--|| "odw_harmonised_db|SAP_HR_ABSENCE_ALL" : "incremental_processing"
    "odw_harmonised_db|SAP_HR_ABSENCE_ALL" ||--|| "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL_TEMP" : "daily_expansion"
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL_TEMP" ||--|| "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL" : "deduplication"
    "odw_harmonised_db|SAP_HR_ABSENCE_ALL" ||--|| "odw_harmonised_db|SAP_HR_FACT_ABSENCE_SICKNESS" : "sickness_grouping"
    "odw_harmonised_db|SAP_HR_WORKSCHEDULERULE" ||--o{ "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL_TEMP" : "provides_2week_schedule"
    "odw_harmonised_db|HIST_SAP_HR" ||--o{ "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL" : "enriches_hr_attributes"
    "odw_standardised_db|LIVE_HOLIDAYS" ||--o{ "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL" : "filters_holidays"
    "odw_standardised_db|LIVE_HOLIDAYS" ||--o{ "odw_harmonised_db|SAP_HR_FACT_ABSENCE_SICKNESS" : "holiday_analysis"
    "odw_harmonised_db|LIVE_DIM_DATE" ||--o{ "odw_harmonised_db|SAP_HR_FACT_ABSENCE_SICKNESS" : "provides_date_intelligence"
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_ALL" ||--|| "odw_curated_db|VW_FACT_ABSENCE_ALL" : "sickness_union"
    "odw_harmonised_db|SAP_HR_FACT_ABSENCE_SICKNESS" ||--|| "odw_curated_db|VW_FACT_ABSENCE_ALL" : "non_sickness_union"
    "odw_standardised_db|WORK_SCHEDULES" ||--o{ "odw_curated_db|VW_FACT_ABSENCE_ALL" : "provides_ons_calculations"
    "odw_curated_db|VW_FACT_ABSENCE_ALL" ||--|| "odw_curated_db|PBI_FACT_ABSENCE_ALL" : "materializes_to"
