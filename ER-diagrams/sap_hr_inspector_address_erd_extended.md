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
        varchar branch
        varchar date_in
        varchar date_out
        varchar eo_responsible
        varchar grade
        varchar resource_group
        varchar staff
        varchar trainee_level
        varchar inspector_group
        float FTE
        varchar primary_location
        varchar primary_FTE
        varchar secondary_location
        varchar secondary_FTE
        varchar InspectorAddress
        varchar Telephoneno
        varchar WorkMobile
        varchar is_sgl
        varchar pins_email_address
        varchar resource_code
        varchar emp_type
        varchar HorizonID
        varchar inspectorSource
        varchar SourceSystemID
        datetime2 IngestionDate
        datetime2 ValidFrom
        datetime2 ValidTo
        varchar RowID
        varchar IsActive
    }
    
    "odw_harmonised_db|LOAD_SAP_HR_MONTHLY" {
        varchar PersNo PK
        varchar Firstname
        varchar Lastname
        varchar EmployeeNo
        varchar CoCd
        varchar CompanyCode
        varchar PA
        varchar PersonnelArea
        varchar PSubarea
        varchar PersonnelSubarea
        varchar Orgunit
        varchar OrganizationalUnit
        varchar Organizationalkey
        varchar OrganizationalKey1
        varchar WorkC
        varchar WorkContract
        varchar CT
        varchar ContractType
        varchar PSgroup
        varchar PayBandDescription
        float FTE
        float Wkhrs
        varchar IndicatorPartTimeEmployee
        varchar S
        varchar EmploymentStatus
        varchar GenderKey
        varchar TRAStartDate
        varchar TRAEndDate
        varchar TRAStatus
        varchar TRAGrade
        varchar PrevPersNo
        varchar ActR
        varchar ReasonforAction
        varchar Position
        varchar Position1
        varchar CostCtr
        varchar CostCentre
        date CivilServiceStart
        date DatetoCurrentJob
        varchar SeniorityDate
        datetime2 DatetoSubstGrade
        varchar PersNo1
        varchar NameofManagerOM
        varchar ManagerPosition
        varchar ManagerPositionText
        varchar CounterSignManager
        varchar Loc
        varchar Location
        date OrgStartDate
        varchar FixTermEndDate
        varchar LoanStartDate
        varchar LoanEndDate
        varchar EEGrp
        varchar EmployeeGroup
        varchar Annualsalary
        varchar Curr
        varchar NInumber
        varchar Birthdate
        varchar Ageofemployee
        varchar EO
        varchar Ethnicorigin
        varchar NID
        varchar Rel
        varchar ReligiousDenominationKey
        varchar SxO
        varchar WageType
        varchar EmployeeSubgroup
        varchar LOAAbsType
        varchar LOAAbsenceTypeText
        varchar Schemereference
        varchar PensionSchemeName
        varchar DisabilityCode
        varchar DisabilityText
        varchar DisabilityCodeDescription
        varchar PArea
        varchar PayrollArea
        varchar AssignmentNumber
        float FTE2
        datetime2 Report_MonthEnd_Date
        datetime2 PDAC_ETL_Date
        varchar SourceSystemID
        datetime2 IngestionDate
        datetime2 ValidTo
        varchar RowID
        varchar IsActive
    }
    
    "odw_harmonised_db|SAP_HR_INSPECTOR_ADDRESS" {
        varchar StaffNumber PK,FK
        varchar StreetandHouseNumber
        varchar AddressLine2
        varchar City
        varchar District
        varchar PostalCode
        varchar Region
        date StartDate
        date EndDate
        varchar ChartingOfficer
        varchar ChartingOfficerforInspector
        varchar SubsPSgroup
        varchar Telephoneno
        varchar PersonalMobile
        varchar WorkMobile
        date Chngdon
        date ValidFrom
        datetime2 ValidTo
        varchar SourceSystemID
        datetime2 IngestionDate
        varchar RowID
        varchar IsActive
    }
    
    "odw_harmonised_db|LOAD_SAP_HR_LEAVERS" {
        varchar PersNo FK
        varchar Lastname
        varchar Firstname
        varchar CoCd
        varchar CompanyCode
        varchar Loc
        varchar Location
        varchar PSgroup
        varchar PayBandDescription
        varchar Orgunit
        varchar OrganizationalUnit
        varchar PA
        varchar PersonnelArea
        varchar PSubarea
        varchar PersonnelSubarea
        varchar WorkC
        varchar WorkContract
        date OrgStartDate
        date Leaving
        varchar Act
        varchar ActionType
        varchar ActR
        varchar ReasonforAction
        varchar S
        varchar EmploymentStatus
        varchar EmployeeNo
        varchar Position
        varchar Position1
        float Annualsalary
        varchar Curr
        varchar UserID
        varchar EmailAddress
        varchar PersNo1
        varchar NameofManagerOM
        varchar ManagerPosition
        varchar ManagerPositionText
        varchar LMEmail
        varchar SourceSystemID
        datetime2 IngestionDate
        datetime2 ValidTo
        varchar RowID
        varchar IsActive
    }
    
    "odw_harmonised_db|LOAD_VW_SAP_HR_EMAIL" {
        varchar email_address FK
        varchar PersNo FK
        varchar Firstname
        varchar Lastname
        varchar EmployeeNo
        varchar CoCd
        varchar CompanyCode
        varchar PA
        varchar PersonnelArea
        varchar PSubarea
        varchar PersonnelSubarea
        varchar Orgunit
        varchar OrganizationalUnit
        varchar Organizationalkey
        varchar OrganizationalKey1
        varchar WorkC
        varchar WorkContract
        varchar CT
        varchar ContractType
        varchar PSgroup
        varchar PayBandDescription
        float FTE
        float Wkhrs
        varchar IndicatorPartTimeEmployee
        varchar S
        varchar EmploymentStatus
        varchar GenderKey
        varchar TRAStartDate
        varchar TRAEndDate
        varchar TRAStatus
        varchar TRAGrade
        varchar PrevPersNo
        varchar ActR
        varchar ReasonforAction
        varchar Position
        varchar Position1
        varchar CostCtr
        varchar CostCentre
        date CivilServiceStart
        varchar DatetoCurrentJob
        varchar SeniorityDate
        date DatetoSubstGrade
        varchar PersNo1
        varchar NameofManagerOM
        varchar ManagerPosition
        varchar ManagerPositionText
        varchar CounterSignManager
        varchar Loc
        varchar Location
        date OrgStartDate
        varchar FixTermEndDate
        varchar LoanStartDate
        varchar LoanEndDate
        varchar EEGrp
        varchar EmployeeGroup
        varchar Annualsalary
        varchar Curr
        varchar NInumber
        varchar Birthdate
        varchar Ageofemployee
        varchar EO
        varchar Ethnicorigin
        varchar NID
        varchar Rel
        varchar ReligiousDenominationKey
        varchar SxO
        varchar WageType
        varchar EmployeeSubgroup
        varchar LOAAbsType
        varchar LOAAbsenceTypeText
        varchar Schemereference
        varchar PensionSchemeName
        varchar DisabilityCode
        varchar DisabilityText
        varchar DisabilityCodeDescription
        varchar PArea
        varchar PayrollArea
        varchar AssignmentNumber
        float FTE2
        date Report_MonthEnd_Date
        date PDAC_ETL_Date
        varchar SourceSystemID
        datetime2 IngestionDate
        datetime2 ValidTo
        varchar RowID
        varchar IsActive
    }
    
    "odw_standardised_db|LOAD_SP_LIST_INSPECTOR_MAP" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar Id PK
        varchar Modified
        varchar Created
        varchar HorizonID
        varchar StaffNumber FK
        varchar horizon_inspector_name
    }
    
    "odw_standardised_db|LOAD_INSPECTOR_RAW" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar Id PK
        varchar Title
        varchar GivenNames
        varchar FamilyName
        varchar Name
        varchar AddressLine1
        varchar AddressLine2
        varchar Town
        varchar County
        varchar Postcode
        varchar PhoneNumber
        varchar EmailAddress
        varchar ActiveStatus
        varchar Branch
        varchar Chart
        varchar DateIn
        varchar DateOut
        varchar EOResponsible
        varchar Grade
        varchar PINSStaffNumber FK
        varchar ResourceGroup
        varchar Staff
        varchar TraineeLevel
        varchar PDAC_ETL_Date
    }
    
    "odw_standardised_db|LOAD_SAPPREFERREDNAME" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar PreferredTitle
        varchar PreferredFirstName
        varchar PreferredFamilyName
        varchar PERSNO FK
    }
    
    "odw_standardised_db|BIS_INSPECTOR_GROUP" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar code
        varchar name
        varchar resource_group
        varchar inspector_group
        varchar New
        varchar Inspector_manager
        varchar Ops_Lead
        varchar sap_ou PK
    }
    
    "odw_standardised_db|HIST_ISS_JOB" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar Id PK
        varchar StartDate
        varchar LeadInspector FK
        varchar Agent
        varchar AllocationReceived
        varchar Appellant
        varchar Branch
        varchar CWT
        varchar Duration
        varchar FileSenttoInspector
        varchar FirstOfferDate
        varchar Job
        varchar JobLastWhenOffered
        varchar Jurisdiction
        varchar JurisdictionName
        varchar LeadJobId
        varchar LPA
        varchar LPAName
        varchar LPARef
        varchar PIMDurn
        varchar PreAdjournedEventDate
        varchar PrepDurn
        varchar Procedure
        varchar ReportingDurn
        varchar SiteAddress
        varchar SitePostcode
        varchar SittingDate
        varchar SittingDurn
        varchar SpecLevel
        varchar Specialisms
        varchar Status
        varchar StatusName
        varchar VisitDurn
        varchar WhenCancelledConfirmedJob
        varchar WhenCancelledUnConfirmedJob
        varchar WhenConfirmed
        varchar is_current
        varchar record_start_date
        varchar record_end_date
    }
    
    "odw_standardised_db|LOAD_ISS_JOB" {
        datetime2 ingested_datetime
        datetime2 expected_from
        datetime2 expected_to
        varchar Id PK
        varchar StartDate
        varchar LeadInspector
        varchar Agent
        varchar AllocationReceived
        varchar Appellant
        varchar Branch
        varchar CWT
        varchar Duration
        varchar FileSenttoInspector
        varchar FirstOfferDate
        varchar Job
        varchar JobLastWhenOffered
        varchar Jurisdiction
        varchar JurisdictionName
        varchar LeadJobId
        varchar LPA
        varchar LPAName
        varchar LPARef
        varchar PIMDurn
        varchar PreAdjournedEventDate
        varchar PrepDurn
        varchar Procedure
        varchar ReportingDurn
        varchar SiteAddress
        varchar SitePostcode
        varchar SittingDate
        varchar SittingDurn
        varchar SpecLevel
        varchar Specialisms
        varchar Status
        varchar StatusName
        varchar VisitDurn
        varchar WhenCancelledConfirmedJob
        varchar WhenCancelledUnConfirmedJob
        varchar WhenConfirmed
        varchar PDAC_ETL_Date
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
