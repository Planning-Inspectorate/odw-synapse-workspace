#### ODW SAPHR Data Model

##### entity: SAP HR Employee

Data model for SAP_HR_Employee Monthly Load data flow from source to harmonised.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class sap_hr_history.csv {
            PersNo: varchar
            EmployeeNo: varchar
            Firstname: varchar
            Lastname: varchar
        }

        class sap_email.csv {
            StaffNumber: varchar
            EmailAddress: varchar
            Firstname: varchar
            Lastname: varchar
        }

    }
    
    namespace Standardised {

        class SAP_HR_MONTHLY {
            PersNo: varchar
            EmployeeNo: varchar
            Firstname: varchar
            Lastname: varchar
            CompanyCode: varchar
            PersonnelArea: varchar
        }
        class SAP_EMAIL_MONTHLY {
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

        class LOAD_VW_SAP_HR_EMAIL {
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
`sap_hr_history.csv` --> `SAP_HR_MONTHLY`
`sap_email.csv` --> `SAP_EMAIL_MONTHLY`

%% Standardised to Harmonised Flow
`SAP_HR_MONTHLY` --> `LOAD_SAP_HR_MONTHLY`
`SAP_EMAIL_MONTHLY` --> `LOAD_SAP_PINS_EMAIL`

%% Harmonised to Harmonised Flow
`LOAD_SAP_HR_MONTHLY` --> `LOAD_VW_SAP_HR_EMAIL`
`SAP_EMAIL_MONTHLY` --> `LOAD_VW_SAP_HR_EMAIL`
`LOAD_SAP_HR_MONTHLY` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL` --> `LIVE_DIM_EMP_HIERARCHY`
`LOAD_SAP_HR_MONTHLY` --> `LIVE_DIM_EMP_HIERARCHY`
