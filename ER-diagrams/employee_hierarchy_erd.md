```mermaid
erDiagram
    LOAD_SAP_HR_MONTHLY {
        varchar PersNo PK
        varchar Firstname
        varchar Lastname
        varchar EmployeeNo
        varchar CoCd
        varchar CompanyCode
        varchar PA
        varchar PersonnelArea
        varchar PSG
        varchar PersonnelSubGroup
        varchar PSA
        varchar PersonnelSubArea
        varchar EmpGrp
        varchar EmployeeGroup
        varchar EmpSubGrp
        varchar EmployeeSubGroup
        varchar PayrollArea
        varchar PayrollAreaTx
        varchar OrgUnit
        varchar OrgUnitTx
        varchar Position
        varchar PositionTx
        varchar Job
        varchar JobTx
        varchar Location
        varchar LocationTx
        varchar CostCtr
        varchar CostCtrTx
        varchar CompCode
        varchar CompCodeTx
        varchar BusinessArea
        varchar BusinessAreaTx
        varchar ProfitCenter
        varchar ProfitCenterTx
        varchar FunctionalArea
        varchar FunctionalAreaTx
        varchar HireDate
        varchar EndDate
        varchar BirthDate
        varchar Age
        varchar Gender
        varchar MaritalStatus
        varchar Nationality
        varchar EmplStatus
        varchar EmplStatusTx
        varchar EmplClass
        varchar EmplClassTx
        varchar ContractType
        varchar ContractTypeTx
        varchar PartTimePercent
        varchar WeeklyHours
        varchar DailyHours
        varchar TimeRecording
        varchar TimeRecordingTx
        varchar Calendar
        varchar CalendarTx
        varchar WorkSchedule
        varchar WorkScheduleTx
        varchar PayScale
        varchar PayScaleTx
        varchar PayGrade
        varchar PayGradeTx
        varchar PayStep
        varchar PayStepTx
        varchar Currency
        varchar BasicPay
        varchar AnnualSalary
        varchar HourlyRate
        varchar BankAccount
        varchar BankKey
        varchar BankName
        varchar PaymentMethod
        varchar PaymentMethodTx
        varchar TaxCode
        varchar TaxCodeTx
        varchar SocialSecNo
        varchar HealthInsurance
        varchar PensionScheme
        varchar UnionMembership
        varchar EmergencyContact
        varchar EmergencyPhone
        varchar HomeAddress
        varchar HomePhone
        varchar MobilePhone
        varchar WorkPhone
        varchar Fax
        varchar Email
        varchar Department
        varchar Team
        varchar Manager
        varchar ManagerEmail
        varchar SkillSet
        varchar Qualifications
        varchar TrainingRecord
        varchar PerformanceRating
        varchar LastPromotion
        varchar NextReview
        varchar Comments
        datetime LoadDate
        varchar SourceSystem
        varchar DataQuality
    }
    
    LOAD_VW_SAP_HR_EMAIL {
        varchar email_address FK
        varchar PersNo FK
        varchar Firstname
        varchar Lastname
        varchar EmployeeNo
        varchar CoCd
        varchar CompanyCode
        varchar PA
        varchar PersonnelArea
        varchar PSG
        varchar PersonnelSubGroup
        varchar PSA
        varchar PersonnelSubArea
        varchar EmpGrp
        varchar EmployeeGroup
        varchar EmpSubGrp
        varchar EmployeeSubGroup
        varchar PayrollArea
        varchar PayrollAreaTx
        varchar OrgUnit
        varchar OrgUnitTx
        varchar Position
        varchar PositionTx
        varchar Job
        varchar JobTx
        varchar Location
        varchar LocationTx
        varchar CostCtr
        varchar CostCtrTx
        varchar CompCode
        varchar CompCodeTx
        varchar BusinessArea
        varchar BusinessAreaTx
        varchar ProfitCenter
        varchar ProfitCenterTx
        varchar FunctionalArea
        varchar FunctionalAreaTx
        varchar HireDate
        varchar EndDate
        varchar BirthDate
        varchar Age
        varchar Gender
        varchar MaritalStatus
        varchar Nationality
        varchar EmplStatus
        varchar EmplStatusTx
        varchar EmplClass
        varchar EmplClassTx
        varchar ContractType
        varchar ContractTypeTx
        varchar PartTimePercent
        varchar WeeklyHours
        varchar DailyHours
        varchar TimeRecording
        varchar TimeRecordingTx
        varchar Calendar
        varchar CalendarTx
        varchar WorkSchedule
        varchar WorkScheduleTx
        varchar PayScale
        varchar PayScaleTx
        varchar PayGrade
        varchar PayGradeTx
        varchar PayStep
        varchar PayStepTx
        varchar Currency
        varchar BasicPay
        varchar AnnualSalary
        varchar HourlyRate
        varchar BankAccount
        varchar BankKey
        varchar BankName
        varchar PaymentMethod
        varchar PaymentMethodTx
        varchar TaxCode
        varchar TaxCodeTx
        varchar SocialSecNo
        varchar HealthInsurance
        varchar PensionScheme
        varchar UnionMembership
        varchar EmergencyContact
        varchar EmergencyPhone
        varchar HomeAddress
        varchar HomePhone
        varchar MobilePhone
        varchar WorkPhone
        varchar Fax
        varchar Email
        varchar Department
        varchar Team
        varchar Manager
        varchar ManagerEmail
        varchar SkillSet
        varchar Qualifications
        varchar TrainingRecord
        varchar PerformanceRating
        varchar LastPromotion
        varchar NextReview
        varchar Comments
        datetime LoadDate
        varchar SourceSystem
        varchar DataQuality
    }
    
    LOAD_SAP_PINS_EMAIL {
        varchar StaffNumber FK
        varchar Firstname
        varchar Lastname
        varchar EmailAddress PK
        varchar SourceSystemID
        datetime2 IngestionDate
        datetime2 ValidTo
        varchar RowID
        varchar IsActive
    }
    
    LIVE_DIM_INSPECTOR {
        varchar source_id PK
        varchar pins_staff_number FK
        varchar given_names
        varchar family_name
        varchar inspector_name
        varchar inspector_postcode
        varchar active_status
        varchar branch
        varchar employee_number
        varchar email_address
        varchar job_title
        varchar job_family
        varchar job_family_group
        varchar directorate
        varchar directorate_group
        varchar grade
        varchar manager_name
        varchar manager_email
        varchar location
        varchar office
        varchar team
        varchar cost_centre
        varchar cost_centre_name
        varchar business_area
        varchar business_area_name
        varchar employment_status
        varchar employment_type
        varchar fte
        varchar start_date
        varchar end_date
        varchar last_updated
        varchar data_source
        varchar record_status
        varchar created_date
        varchar modified_date
        varchar created_by
        varchar modified_by
        varchar version_number
        varchar is_current
        varchar effective_from
        varchar effective_to
    }
    
    LIVE_DIM_EMP_HIERARCHY {
        varchar emp_id FK
        varchar emp_name
        varchar emp_email_address FK
        varchar mgr_id
        varchar mgr_name
        varchar mgr_level
        varchar mgr_email_address
        varchar hierarchy
        varchar department
        varchar division
        varchar cost_centre
        varchar business_unit
        varchar reporting_chain
        varchar effective_date
    }

    %% Primary Data Flow Relationships
    LOAD_SAP_HR_MONTHLY ||--o{ LOAD_VW_SAP_HR_EMAIL : "email_view"
    LOAD_SAP_HR_MONTHLY ||--o{ LOAD_SAP_PINS_EMAIL : "pins_integration"
    LOAD_SAP_HR_MONTHLY ||--o{ LIVE_DIM_INSPECTOR : "inspector_subset"
    LOAD_SAP_HR_MONTHLY ||--o{ LIVE_DIM_EMP_HIERARCHY : "hierarchy_member"
    
    %% Cross-Reference Relationships  
    LOAD_SAP_PINS_EMAIL ||--o{ LOAD_VW_SAP_HR_EMAIL : "email_mapping"
    LOAD_VW_SAP_HR_EMAIL ||--o{ LIVE_DIM_EMP_HIERARCHY : "email_hierarchy_ref"
