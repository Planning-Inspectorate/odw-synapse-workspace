```mermaid
erDiagram
    "odw_harmonised_db|load_SAP_HR_Leavers" {
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
    
    "odw_harmonised_db|stage_SAP_HR_Leavers" {
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
    
    "odw_standardised_db|sap_hr_leavers_monthly" {
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
    
    "odw_standardised_db|sap_hr_leavers_monthly" ||--|| "odw_harmonised_db|stage_SAP_HR_Leavers" : "transforms_to"
    "odw_harmonised_db|stage_SAP_HR_Leavers" ||--|| "odw_harmonised_db|load_SAP_HR_Leavers" : "loads_to"