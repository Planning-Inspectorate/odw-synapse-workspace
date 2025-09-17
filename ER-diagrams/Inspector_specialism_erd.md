```mermaid
erDiagram
    "odw_standardised_db|INSPECTOR_SPECIALISMS_MONTHLY" {
		date ingested_datetime
		date expected_from
		date expected_to
		string StaffNumber PK
		string Firstname
		string Lastname
		string QualificationName PK
		string Proficien
		
    }
    
    "odw_harmonised_db|TRANSFORM_INSPECTOR_SPECIALISMS" {
		string StaffNumber PK
		string Firstname
		string Lastname
		string QualificationName  PK
		string Proficien
		string SourceSystemID
		date IngestionDate
		date ValidTo
		string RowID
		string IsActive
    }
    
    "odw_harmonised_db|SAP_HR_INSPECTOR_SPECIALISMS" {
        string StaffNumber PK
        string Firstname
        string Lastname
        string QualificationName PK
        string Proficien
        string SourceSystemID
        date IngestionDate
        date ValidFrom
        date ValidTo
		date LastUpdated
        int Current
        string RowID
        string IsActive		
    }
    
    "odw_standardised_db|INSPECTOR_SPECIALISMS_MONTHLY" ||--|| "odw_harmonised_db|TRANSFORM_INSPECTOR_SPECIALISMS" : "transforms_to_weekly_monthly"
    "odw_harmonised_db|TRANSFORM_INSPECTOR_SPECIALISMS" ||--|| "odw_harmonised_db|SAP_HR_INSPECTOR_SPECIALISMS" : "loads_to_weekly_monthly"
