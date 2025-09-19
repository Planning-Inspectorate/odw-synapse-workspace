```mermaid
classDiagram

    direction LR

    namespace Sources {

        class SAP_HR_Source {
            PersNo: varchar
            EmployeeNo: varchar
            Firstname: varchar
            Lastname: varchar
        }

        class SAP_PINS_Source {
            StaffNumber: varchar
            EmailAddress: varchar
            Firstname: varchar
            Lastname: varchar
        }

    }
    
    namespace Standardised {

        class sap_hr_monthly_std {
            PersNo: varchar
            EmployeeNo: varchar
            Firstname: varchar
            Lastname: varchar
            CompanyCode: varchar
            PersonnelArea: varchar
        }

        class sap_hr_weekly_std {
            PersNo: varchar
            EmployeeNo: varchar
            Firstname: varchar
            Lastname: varchar
            CompanyCode: varchar
            PersonnelArea: varchar
        }

        class sap_pins_email_std {
            StaffNumber: varchar
            EmailAddress: varchar
            Firstname: varchar
            Lastname: varchar
        }

    }

    namespace Harmonised {

        class LOAD_SAP_HR_MONTHLY {
            PersNo: varchar
            Firstname: varchar
            Lastname: varchar
            EmployeeNo: varchar
            Email: varchar
            Manager: varchar
            Department: varchar
            LoadDate: datetime
        }

        class LOAD_SAP_HR_WEEKLY {
            PersNo: varchar
            Firstname: varchar
            Lastname: varchar
            EmployeeNo: varchar
            Email: varchar
            Manager: varchar
            Department: varchar
            LoadDate: datetime
        }

        class LOAD_VW_SAP_HR_EMAIL {
            email_address: varchar
            PersNo: varchar
            Firstname: varchar
            Lastname: varchar
            EmployeeNo: varchar
        }

        class LOAD_VW_SAP_HR_EMAIL_WEEKLY {
            email_address: varchar
            PersNo: varchar
            Firstname: varchar
            Lastname: varchar
            EmployeeNo: varchar
        }

        class LOAD_SAP_PINS_EMAIL {
            StaffNumber: varchar
            EmailAddress: varchar
            Firstname: varchar
            Lastname: varchar
            SourceSystemID: varchar
        }

        class LOAD_SAP_PINS_EMAIL_WEEKLY {
            StaffNumber: varchar
            EmailAddress: varchar
            Firstname: varchar
            Lastname: varchar
            SourceSystemID: varchar
        }

        class LIVE_DIM_INSPECTOR {
            source_id: varchar
            pins_staff_number: varchar
            inspector_name: varchar
            email_address: varchar
            manager_name: varchar
            directorate: varchar
            grade: varchar
            active_status: varchar
        }

        class LIVE_DIM_EMP_HIERARCHY {
            emp_id: varchar
            emp_name: varchar
            emp_email_address: varchar
            mgr_id: varchar
            mgr_name: varchar
            hierarchy: varchar
            department: varchar
            reporting_chain: varchar
        }		
    
    }

    namespace Curated {

        class LIVE_DIM_INSPECTOR {
            source_id: varchar
            pins_staff_number: varchar
            inspector_name: varchar
            email_address: varchar
            manager_name: varchar
            directorate: varchar
            grade: varchar
            active_status: varchar
        }

        class LIVE_DIM_EMP_HIERARCHY {
            emp_id: varchar
            emp_name: varchar
            emp_email_address: varchar
            mgr_id: varchar
            mgr_name: varchar
            hierarchy: varchar
            department: varchar
            reporting_chain: varchar
        }
    }

%% Source to Standardised Flow
SAP_HR_Source --> sap_hr_monthly_std
SAP_HR_Source --> sap_hr_weekly_std
SAP_PINS_Source --> sap_pins_email_std

%% Standardised to Harmonised Flow
sap_hr_monthly_std --> LOAD_SAP_HR_MONTHLY
sap_hr_weekly_std --> LOAD_SAP_HR_WEEKLY
sap_pins_email_std --> LOAD_SAP_PINS_EMAIL
sap_pins_email_std --> LOAD_SAP_PINS_EMAIL_WEEKLY

%% Harmonised Internal Flow
LOAD_SAP_HR_MONTHLY --> LOAD_VW_SAP_HR_EMAIL
LOAD_SAP_HR_WEEKLY --> LOAD_VW_SAP_HR_EMAIL_WEEKLY

%% Harmonised to Curated Flow
LOAD_SAP_HR_MONTHLY --> LIVE_DIM_INSPECTOR
LOAD_SAP_HR_WEEKLY --> LIVE_DIM_INSPECTOR
LOAD_SAP_PINS_EMAIL --> LIVE_DIM_INSPECTOR
LOAD_SAP_PINS_EMAIL_WEEKLY --> LIVE_DIM_INSPECTOR

LOAD_VW_SAP_HR_EMAIL --> LIVE_DIM_EMP_HIERARCHY
LOAD_VW_SAP_HR_EMAIL_WEEKLY --> LIVE_DIM_EMP_HIERARCHY
LOAD_SAP_HR_MONTHLY --> LIVE_DIM_EMP_HIERARCHY
LOAD_SAP_HR_WEEKLY --> LIVE_DIM_EMP_HIERARCHY