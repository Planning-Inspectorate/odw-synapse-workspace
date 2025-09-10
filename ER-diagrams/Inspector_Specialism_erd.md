```mermaid
erDiagram
    "odw_standardised_db.INSPECTOR_SPECIALISMS_MONTHLY" {
        string StaffNumber PK
        string Firstname
        string Lastname
        string QualificationName PK
        string Proficien
    }
    
    "odw_harmonised_db.TRANSFORM_INSPECTOR_SPECIALISMS" {
        string StaffNumber PK
        string Firstname
        string Lastname
        string QualificationName PK
        string Proficien
        string SourceSystemID
        date IngestionDate
        date ValidTo
        string RowID
        string IsActive
    }
    
    "odw_harmonised_db.SAP_HR_INSPECTOR_SPECIALISMS" {
        string StaffNumber PK
        string Firstname
        string Lastname
        string QualificationName PK
        string Proficien
        string SourceSystemID
        date IngestionDate
        date ValidFrom
        date ValidTo
        int Current
        string RowID
        string IsActive
        date LastUpdated
    }
    
    inspector_specialisms_monthly ||--|| transform_inspector_Specialisms : "transforms_to"
    transform_inspector_Specialisms ||--|| sap_hr_inspector_Specialisms : "loads_to"