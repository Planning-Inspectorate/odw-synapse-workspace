```mermaid
erDiagram
    "odw_standardised_db|sap_protected_monthly" {
		date ingested_datetime
		date expected_from
		date expected_to
        string RefNo PK
        string EthnicOrigin
        string ReligiousDenominationKey
        string SxO
        string Grade
        string DisabilityText
		string DisabilityCodeDescription
		string Disabled
		string Ethnicity
		string Religion
		string SexualOrientation
        string Report_MonthEnd_Date		
    }
    
    "odw_harmonised_db|SAP_HR_PC" {
        string RefNo PK
        string EthnicOrigin
        string ReligiousDenominationKey
        string SxO
        string Grade
        string DisabilityText
        date Report_MonthEnd_Date PK
        string SourceSystemID
        date IngestionDate
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|sap_hr_protected_data" {
        string RefNo PK
        string EthnicOrigin
        string ReligiousDenominationKey
        string SxO
        string Grade
        string DisabilityText
        date Report_MonthEnd_Date PK
        string SourceSystemID
        date IngestionDate
        timestamp ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db|live_dim_date" {
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
    
    "odw_curated_db|vw_HR_ProtectedData" {
        date PC_Date PK, FK
        string PC_Month
        int month_int
        string PC_Month_Latest
        bigint MonthYearLatest_SortKey
        int PC_Year
        int PC_FY
        string PC_FY_Latest
        int FY_Latest_SortKey
        string Ref_Number PK
        string DisabilityText
        string Disabled
        string Disability_group
        string EthnicOrigin
        string Ethnicity
        string Ethnicity_group
        string ReligiousDenominationKey
        string Religion
        string Religion_group
        string SxO
        string Sexual_Orientation
        string Sexual_Orientation_group
        int PC_Headcount
        string Data_Completeness
    }
    
    "odw_curated_db|pbi_HR_ProtectedData" {
        date PC_Date PK, FK
        string PC_Month
        int month_int
        string PC_Month_Latest
        bigint MonthYearLatest_SortKey
        int PC_Year
        int PC_FY
        string PC_FY_Latest
        int FY_Latest_SortKey
        string Ref_Number PK
        string DisabilityText
        string Disabled
        string Disability_group
        string EthnicOrigin
        string Ethnicity
        string Ethnicity_group
        string ReligiousDenominationKey
        string Religion
        string Religion_group
        string SxO
        string Sexual_Orientation
        string Sexual_Orientation_group
        int PC_Headcount
        string Data_Completeness
    }
    
    "odw_standardised_db|sap_protected_monthly" ||--|| "odw_harmonised_db|SAP_HR_PC" : "transforms_to"
    "odw_harmonised_db|SAP_HR_PC" ||--|| "odw_harmonised_db|sap_hr_protected_data" : "merges_to"
    "odw_harmonised_db|sap_hr_protected_data" ||--|| "odw_curated_db|vw_HR_ProtectedData" : "transforms_to"
    "odw_harmonised_db|live_dim_date" ||--o{ "odw_curated_db|vw_HR_ProtectedData" : "provides_date_attributes"
    "odw_curated_db|vw_HR_ProtectedData" ||--|| "odw_curated_db|pbi_HR_ProtectedData" : "materializes_to"