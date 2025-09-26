classDiagram

    direction LR

    namespace Sources {

        class sap_hr_monthly.csv {
            PersNo: varchar
            EmployeeNo: varchar
            FirstName: varchar
            LastName: varchar
            OrganizationalUnit: varchar
            PSgroup: varchar
        }

        class sap_hr_leavers.csv {
            PersNo: varchar
            FirstName: varchar
            LastName: varchar
            Leaving: datetime
        }

        class inspector_raw.csv {
            PINSStaffNumber: int
            GivenNames: varchar
            FamilyName: varchar
            ActiveStatus: varchar
            Grade: varchar
        }

        class inspector_address.csv {
            StaffNumber: varchar
            PostalCode: varchar
            StreetandHouseNumber: varchar
            City: varchar
            District: varchar
        }

    }
    
    namespace Standardised {

        class LOAD_SP_LIST_INSPECTOR_MAP {
            StaffNumber: varchar
            HorizonID: varchar
            Modified: datetime
        }

        class LOAD_INSPECTOR_RAW {
            id: int
            PINSStaffNumber: int
            GivenNames: varchar
            FamilyName: varchar
            ActiveStatus: varchar
            Grade: varchar
            ResourceGroup: varchar
        }

        class LOAD_SAPPREFERREDNAME {
            PERSNO: varchar
            PreferredFirstName: varchar
            PreferredFamilyName: varchar
        }

        class BIS_INSPECTOR_GROUP {
            sap_ou: varchar
            resource_group: varchar
            inspector_group: varchar
        }

        class HIST_ISS_JOB {
            ID: varchar
            LeadInspector: varchar
            record_start_date: datetime
            record_end_date: datetime
        }

        class LOAD_ISS_JOB {
            ID: varchar
            LeadInspector: varchar
            Status: varchar
        }

    }

    namespace Harmonised {

        class LOAD_SAP_HR_MONTHLY {
            PersNo: varchar
            EmployeeNo: varchar
            FirstName: varchar
            LastName: varchar
            OrganizationalUnit: varchar
            PSgroup: varchar
            WorkContract: varchar
            Position1: varchar
            Location: varchar
            EmploymentStatus: varchar
            FTE: decimal
        }

        class LOAD_SAP_HR_LEAVERS {
            PersNo: varchar
            FirstName: varchar
            LastName: varchar
            Leaving: datetime
            PSgroup: varchar
            WorkContract: varchar
            OrganizationalUnit: varchar
        }

        class SAP_HR_INSPECTOR_ADDRESS {
            StaffNumber: varchar
            PostalCode: varchar
            StreetandHouseNumber: varchar
            City: varchar
            District: varchar
            ChartingOfficerforInspector: varchar
            Telephoneno: varchar
            WorkMobile: varchar
            IsActive: varchar
        }

        class LOAD_VW_SAP_HR_EMAIL {
            PersNo: varchar
            email_address: varchar
            EmployeeNo: varchar
        }

        class LIVE_DIM_INSPECTOR {
            source_id: varchar
            pins_staff_number: varchar
            given_names: varchar
            family_name: varchar
            inspector_name: varchar
            inspector_postcode: varchar
            active_status: varchar
            grade: varchar
            resource_group: varchar
            FTE: varchar
            primary_location: varchar
            InspectorAddress: varchar
            Telephoneno: varchar
            WorkMobile: varchar
            pins_email_address: varchar
            resource_code: varchar
            emp_type: varchar
            HorizonID: varchar
            inspectorSource: varchar
        }
    
    }

%% Source to Standardised Flow
`inspector_raw.csv` --> `LOAD_INSPECTOR_RAW`
`sap_hr_monthly.csv` --> `LOAD_SAPPREFERREDNAME`
`sap_hr_monthly.csv` --> `BIS_INSPECTOR_GROUP`

%% Source to Harmonised Flow
`sap_hr_monthly.csv` --> `LOAD_SAP_HR_MONTHLY`
`sap_hr_leavers.csv` --> `LOAD_SAP_HR_LEAVERS`
`inspector_address.csv` --> `SAP_HR_INSPECTOR_ADDRESS`

%% Standardised to Harmonised Flow
`LOAD_INSPECTOR_RAW` --> LIVE_DIM_INSPECTOR`
`LOAD_SP_LIST_INSPECTOR_MAP` --> `LIVE_DIM_INSPECTOR`
`LOAD_SAPPREFERREDNAME` --> `LIVE_DIM_INSPECTOR`
`BIS_INSPECTOR_GROUP` --> `LIVE_DIM_INSPECTOR`
`HIST_ISS_JOB` --> `LOAD_ISS_JOB`

%% Harmonised to Harmonised Flow
`LOAD_SAP_HR_MONTHLY` --> `LOAD_VW_SAP_HR_EMAIL`
`LOAD_SAP_HR_MONTHLY` --> `SAP_HR_INSPECTOR_ADDRESS`
`LOAD_SAP_HR_MONTHLY` --> `LIVE_DIM_INSPECTOR`
`LOAD_SAP_HR_LEAVERS` --> `LIVE_DIM_INSPECTOR`
`SAP_HR_INSPECTOR_ADDRESS` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL` --> `LIVE_DIM_INSPECTOR`
`LOAD_ISS_JOB` --> `HIST_ISS_JOB`