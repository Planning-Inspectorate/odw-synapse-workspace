#### ODW SAPHR Data Model

##### entity: SAP HR Employee

Data model for SAP_HR_Employee Weekly Load data flow from source to harmonised.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class sap_hr.csv {
            PersNo: string
            EmployeeNo: string
            Firstname: string
            Lastname: string
        }

        class sap_email.csv {
            StaffNumber: string
            EmailAddress: string
            Firstname: string
            Lastname: string
        }

    }
    
    namespace Standardised {

        class SAP_HR_WEEKLY {
            PersNo: string
            EmployeeNo: string
            Firstname: string
            Lastname: string
            CompanyCode: string
            PersonnelArea: string
        }
        class SAP_EMAIL_WEEKLY {
            StaffNumber: string
            EmailAddress: string
            Firstname: string
            Lastname: string
        }

    }

    namespace Harmonised {

        class LOAD_SAP_HR_WEEKLY {
            PersNo: string
            Firstname: string
            Lastname: string
            EmployeeNo: string
            Email: string
            Manager: string
            Department: string
            LoadDate: datetime
        }

        class LOAD_VW_SAP_HR_EMAIL_WEEKLY {
            email_address: string
            PersNo: string
            Firstname: string
            Lastname: string
            EmployeeNo: string
        }

        class LOAD_SAP_PINS_EMAIL_WEEKLY {
            StaffNumber: string
            EmailAddress: string
            Firstname: string
            Lastname: string
            SourceSystemID: string
        }

        class LIVE_DIM_INSPECTOR {
            source_id: string
            pins_staff_number: string
            inspector_name: string
            email_address: string
            manager_name: string
            directorate: string
            grade: string
            active_status: string
        }

        class LIVE_DIM_EMP_HIERARCHY {
            emp_id: string
            emp_name: string
            emp_email_address: string
            mgr_id: string
            mgr_name: string
            hierarchy: string
            department: string
            reporting_chain: string
        }		
    
    }

%% Source to Standardised Flow
`sap_hr.csv` --> `SAP_HR_WEEKLY`
`sap_email.csv` --> `SAP_EMAIL_WEEKLY`

%% Standardised to Harmonised Flow
`SAP_HR_WEEKLY` --> `LOAD_SAP_HR_WEEKLY`
`SAP_EMAIL_WEEKLY` --> `LOAD_SAP_PINS_EMAIL_WEEKLY`

%% Harmonised to Harmonised Flow
`LOAD_SAP_HR_WEEKLY` --> `LOAD_VW_SAP_HR_EMAIL_WEEKLY`
`SAP_EMAIL_WEEKLY` --> `LOAD_VW_SAP_HR_EMAIL_WEEKLY`
`LOAD_SAP_HR_WEEKLY` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL_WEEKLY` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL_WEEKLY` --> `LIVE_DIM_EMP_HIERARCHY`
`LOAD_SAP_HR_WEEKLY` --> `LIVE_DIM_EMP_HIERARCHY`
