```mermaid
erDiagram
    "odw_harmonised_db|LIVE_DIM_INSPECTOR" {
        varchar source_id PK
        varchar pins_staff_number FK
        varchar given_names
        varchar family_name
        varchar inspector_name
        varchar inspector_postcode
        varchar active_status
        varchar grade
        varchar resource_group
        varchar FTE
        varchar primary_location
        varchar InspectorAddress
        varchar Telephoneno
        varchar WorkMobile
        varchar pins_email_address
        varchar resource_code
        varchar emp_type
        varchar HorizonID
        varchar inspectorSource
    }
    
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" {
        varchar PersNo PK
        varchar EmployeeNo
        varchar FirstName
        varchar LastName
        varchar OrganizationalUnit
        varchar PSgroup
        varchar WorkContract
        varchar Position1
        varchar Location
        varchar EmploymentStatus
        varchar OrgStartDate
        decimal FTE
    }
    
    "odw_harmonised_db|SAP_HR_INSPECTOR_ADDRESS" {
        varchar StaffNumber FK
        varchar PostalCode
        varchar StreetandHouseNumber
        varchar City
        varchar District
        varchar ChartingOfficerforInspector
        varchar Telephoneno
        varchar WorkMobile
        varchar IsActive
    }
    
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" {
        varchar PersNo FK
        varchar FirstName
        varchar LastName
        datetime Leaving
        varchar PSgroup
        varchar WorkContract
        varchar OrganizationalUnit
    }
    
    "odw_harmonised_db|LOAD_VW_SAP_HR_EMAIL" {
        varchar PersNo FK
        varchar email_address
        varchar EmployeeNo
    }
    
    "odw_standardised_db|LOAD_SP_LIST_INSPECTOR_MAP" {
        varchar StaffNumber FK
        varchar HorizonID
        datetime Modified
    }
    
    "odw_standardised_db|LOAD_INSPECTOR_RAW" {
        int id PK
        int PINSStaffNumber
        varchar GivenNames
        varchar FamilyName
        varchar ActiveStatus
        varchar Grade
        varchar ResourceGroup
    }
    
    "odw_standardised_db|LOAD_SAPPREFERREDNAME" {
        varchar PERSNO FK
        varchar PreferredFirstName
        varchar PreferredFamilyName
    }
    
    "odw_standardised_db|BIS_INSPECTOR_GROUP" {
        varchar sap_ou PK
        varchar resource_group
        varchar inspector_group
    }
    
    "odw_standardised_db|HIST_ISS_JOB" {
        varchar ID PK
        varchar LeadInspector
        datetime record_start_date
        datetime record_end_date
    }
    
    "odw_standardised_db|LOAD_ISS_JOB" {
        varchar ID PK
        varchar LeadInspector
        varchar Status
    }

    %% PRIMARY RELATIONSHIPS - Source tables feeding LIVE_DIM_INSPECTOR
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "PersNo-pins_staff_number"
    "odw_harmonised_db|SAP_HR_INSPECTOR_ADDRESS" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "StaffNumber-pins_staff_number"
    
    %% Supporting source relationships for LIVE_DIM_INSPECTOR
    "odw_standardised_db|LOAD_SP_LIST_INSPECTOR_MAP" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "StaffNumber-pins_staff_number"
    "odw_standardised_db|LOAD_INSPECTOR_RAW" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "PINSStaffNumber-pins_staff_number"
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "PersNo-pins_staff_number"
    "odw_standardised_db|BIS_INSPECTOR_GROUP" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "sap_ou-resource_group"
    "odw_harmonised_db|LOAD_VW_SAP_HR_EMAIL" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "PersNo-pins_staff_number"
    "odw_standardised_db|LOAD_SAPPREFERREDNAME" ||--o{ "odw_harmonised_db|LIVE_DIM_INSPECTOR" : "PERSNO-pins_staff_number"
    
    %% Core source table relationships
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--o{ "odw_harmonised_db|SAP_HR_INSPECTOR_ADDRESS" : "PersNo-StaffNumber"
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--o{ "odw_standardised_db|LOAD_SP_LIST_INSPECTOR_MAP" : "PersNo-StaffNumber"
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--o{ "odw_harmonised_db|LOAD_VW_SAP_HR_EMAIL" : "PersNo-PersNo"
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--o{ "odw_standardised_db|LOAD_SAPPREFERREDNAME" : "PersNo-PERSNO"
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" ||--|| "odw_standardised_db|BIS_INSPECTOR_GROUP" : "OrganizationalUnit-sap_ou"
    
    %% Target table usage in horizon appeals
    "odw_harmonised_db|LIVE_DIM_INSPECTOR" ||--o{ "odw_standardised_db|HIST_ISS_JOB" : "source_id-LeadInspector"
    "odw_standardised_db|HIST_ISS_JOB" ||--|| "odw_standardised_db|LOAD_ISS_JOB" : "ID-ID"
