```mermaid
classDiagram

    direction LR

    namespace Sources {

        class sap_hr.csv {
            PersNo: string
            EmployeeNo: string
            FirstName: string
            LastName: string
            OrganizationalUnit: string
            PSgroup: string
        }
		
        class sap_hr_leavers.csv {
            PersNo: string
            FirstName: string
            LastName: string
            Leaving: datetime
        }

        class inspector_raw.csv {
            PINSStaffNumber: int
            GivenNames: string
            FamilyName: string
            ActiveStatus: string
            Grade: string
        }

        class inspector_address.csv {
            StaffNumber: string
            PostalCode: string
            StreetandHouseNumber: string
            City: string
            District: string
        }

        class sappreferredname.csv {
            PERSNO: string
            PreferredFirstName: string
            PreferredFamilyName: string
        }

        class bis_inspector_group.csv {
            sap_ou: string
            resource_group: string
            inspector_group: string
        }
		
        class load_iss_deployment.csv {
			StaffNumber	string            
			SGLName: string
			SubGroup: string
			Name: string
			IsSGL: string
			Date: string
        }

        class sp_list_inspector_map.csv {
            StaffNumber: string
            HorizonID: string
            Modified: datetime
        }		
    }
    
    namespace Standardised {

        class LOAD_SP_LIST_INSPECTOR_MAP {
            StaffNumber: string
            HorizonID: string
            Modified: datetime
        }
		
        class SAP_HR_LEAVERS {
            PersNo: string
            FirstName: string
            LastName: string
            Leaving: datetime
        }		

        class LOAD_INSPECTOR_RAW {
            id: int
            PINSStaffNumber: int
            GivenNames: string
            FamilyName: string
            ActiveStatus: string
            Grade: string
            ResourceGroup: string
        }

        class LOAD_SAPPREFERREDNAME {
            PERSNO: string
            PreferredFirstName: string
            PreferredFamilyName: string
        }

        class BIS_INSPECTOR_GROUP {
            sap_ou: string
            resource_group: string
            inspector_group: string
        }

        class LOAD_ISS_DEPLOYMENT {
			StaffNumber	string            
			SGLName: string
			SubGroup: string
			Name: string
			IsSGL: string
			Date: string
        }

		class SAP_HR_WEEKLY {
            PersNo: string
            EmployeeNo: string
            FirstName: string
            LastName: string
            OrganizationalUnit: string
            PSgroup: string
            WorkContract: string
            Position1: string
            Location: string
            EmploymentStatus: string
            FTE: float
        }		

        class INSPECTOR_ADDRESS {
            StaffNumber: string
            PostalCode: string
            StreetandHouseNumber: string
            City: string
            District: string
            ChartingOfficerforInspector: string
            Telephoneno: string
            WorkMobile: string
            IsActive: string
        }		
    }

    namespace Harmonised {

		class LOAD_SAP_HR_WEEKLY {
            PersNo: string
            EmployeeNo: string
            FirstName: string
            LastName: string
            OrganizationalUnit: string
            PSgroup: string
            WorkContract: string
            Position1: string
            Location: string
            EmploymentStatus: string
            FTE: float
        }		

        class LOAD_SAP_HR_LEAVERS {
            PersNo: string
            FirstName: string
            LastName: string
            Leaving: datetime
            PSgroup: string
            WorkContract: string
            OrganizationalUnit: string
        }

        class SAP_HR_INSPECTOR_ADDRESS {
            StaffNumber: string
            PostalCode: string
            StreetandHouseNumber: string
            City: string
            District: string
            ChartingOfficerforInspector: string
            Telephoneno: string
            WorkMobile: string
            IsActive: string
        }

        class LOAD_VW_SAP_HR_EMAIL_WEEKLY {
            PersNo: string
            email_address: string
            EmployeeNo: string
        }		

        class LIVE_DIM_INSPECTOR {
            source_id: string
            pins_staff_number: string
            given_names: string
            family_name: string
            inspector_name: string
            inspector_postcode: string
            active_status: string
            grade: string
            resource_group: string
            FTE: string
            primary_location: string
            InspectorAddress: string
            Telephoneno: string
            WorkMobile: string
            pins_email_address: string
            resource_code: string
            emp_type: string
            HorizonID: string
            inspectorSource: string
        }
    
    }

%% Source to Standardised Flow
`inspector_raw.csv` --> `LOAD_INSPECTOR_RAW`
`sap_hr.csv` --> `SAP_HR_WEEKLY`
`sappreferredname.csv` --> `LOAD_SAPPREFERREDNAME`
`bis_inspector_group.csv` --> `BIS_INSPECTOR_GROUP`
`inspector_address.csv` --> `INSPECTOR_ADDRESS`
`sap_hr_leavers.csv` --> `SAP_HR_LEAVERS`
`sp_list_inspector_map.csv` --> `LOAD_SP_LIST_INSPECTOR_MAP`
`load_iss_deployment.csv` --> `LOAD_ISS_DEPLOYMENT`

%% Standardised to Harmonised Flow
`SAP_HR_WEEKLY` --> `LOAD_SAP_HR_WEEKLY`
`SAP_HR_LEAVERS` --> `LOAD_SAP_HR_LEAVERS`
`INSPECTOR_ADDRESS` --> `SAP_HR_INSPECTOR_ADDRESS`
`LOAD_INSPECTOR_RAW` --> `LIVE_DIM_INSPECTOR`
`LOAD_SP_LIST_INSPECTOR_MAP` --> `LIVE_DIM_INSPECTOR`
`LOAD_SAPPREFERREDNAME` --> `LIVE_DIM_INSPECTOR`
`BIS_INSPECTOR_GROUP` --> `LIVE_DIM_INSPECTOR`

%% Harmonised to Harmonised Flow
`LOAD_SAP_HR_WEEKLY` --> `LOAD_VW_SAP_HR_EMAIL_WEEKLY`
`LOAD_SAP_HR_WEEKLY` --> `SAP_HR_INSPECTOR_ADDRESS`
`LOAD_SAP_HR_WEEKLY` --> `LIVE_DIM_INSPECTOR`
`LOAD_SAP_HR_LEAVERS` --> `LIVE_DIM_INSPECTOR`
`SAP_HR_INSPECTOR_ADDRESS` --> `LIVE_DIM_INSPECTOR`
`LOAD_VW_SAP_HR_EMAIL_WEEKLY` --> `LIVE_DIM_INSPECTOR`
