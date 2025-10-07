#### ODW SAPHR Data Model

##### entity: SAP_HR_LEAVERS_MONTHLY

Data model for SAP HR Leavers entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class sap_hr_leavers.csv {
            PersNo: string
            Lastname: string
            Firstname: string
            CoCd: string
            CompanyCode: string
            Loc: string
            Location: string
            PSgroup: string
            PayBandDescription: string
            Orgunit: string
            OrganizationalUnit: string
            PA: string
            PersonnelArea: string
            PSubarea: string
            PersonnelSubarea: string
            WorkC: string
            WorkContract: string
            OrgStartDate: date
            Leaving: date
            Act: string
            ActionType: string
            ActR: string
            ReasonforAction: string
            S: string
            EmploymentStatus: string
            EmployeeNo: string
            Position: string
            Position1: string
            Annualsalary: float
            Curr: string
            UserID: string
            EmailAddress: string
            PersNo1: string
            NameofManagerOM: string
            ManagerPosition: string
            ManagerPositionText: string
            LMEmail: string
        }
    }
    
    namespace Standardised {

        class SAP_HR_LEAVERS_MONTHLY {
            PersNo: string
            Lastname: string
            Firstname: string
            CoCd: string
            CompanyCode: string
            Loc: string
            Location: string
            PSgroup: string
            PayBandDescription: string
            Orgunit: string
            OrganizationalUnit: string
            PA: string
            PersonnelArea: string
            PSubarea: string
            PersonnelSubarea: string
            WorkC: string
            WorkContract: string
            OrgStartDate: string
            Leaving: string
            Act: string
            ActionType: string
            ActR: string
            ReasonforAction: string
            S: string
            EmploymentStatus: string
            EmployeeNo: string
            Position: string
            Position1: string
            Annualsalary: string
            Curr: string
            UserID: string
            EmailAddress: string
            PersNo1: string
            NameofManagerOM: string
            ManagerPosition: string
            ManagerPositionText: string
            LMEmail: string
        }
    }

    namespace Harmonised {

        class STAGE_SAP_HR_LEAVERS {
            PersNo: string
            Lastname: string
            Firstname: string
            CoCd: string
            CompanyCode: string
            Loc: string
            Location: string
            PSgroup: string
            PayBandDescription: string
            Orgunit: string
            OrganizationalUnit: string
            PA: string
            PersonnelArea: string
            PSubarea: string
            PersonnelSubarea: string
            WorkC: string
            WorkContract: string
            OrgStartDate: date
            Leaving: date
            Act: string
            ActionType: string
            ActR: string
            ReasonforAction: string
            S: string
            EmploymentStatus: string
            EmployeeNo: string
            Position: string
            Position1: string
            Annualsalary: double
            Curr: string
            UserID: string
            EmailAddress: string
            PersNo1: string
            NameofManagerOM: string
            ManagerPosition: string
            ManagerPositionText: string
            LMEmail: string
            SourceSystemID: string
            IngestionDate: date
            ValidTo: timestamp
            RowID: string
            IsActive: string
        }

        class LOAD_SAP_HR_LEAVERS {
            PersNo: string
            Lastname: string
            Firstname: string
            CoCd: string
            CompanyCode: string
            Loc: string
            Location: string
            PSgroup: string
            PayBandDescription: string
            Orgunit: string
            OrganizationalUnit: string
            PA: string
            PersonnelArea: string
            PSubarea: string
            PersonnelSubarea: string
            WorkC: string
            WorkContract: string
            OrgStartDate: date
            Leaving: date
            Act: string
            ActionType: string
            ActR: string
            ReasonforAction: string
            S: string
            EmploymentStatus: string
            EmployeeNo: string
            Position: string
            Position1: string
            Annualsalary: float
            Curr: string
            UserID: string
            EmailAddress: string
            PersNo1: string
            NameofManagerOM: string
            ManagerPosition: string
            ManagerPositionText: string
            LMEmail: string
            SourceSystemID: string
            IngestionDate: date
            ValidTo: date
            RowID: string
            IsActive: string
        }

        class HIST_SAP_HR {
            PersNo: string
            Report_MonthEnd_Date: date
            Firstname: string
            Lastname: string
            EmployeeNo: string
            CompanyCode: string
            PersonnelArea: string
            PersonnelSubarea: string
            OrganizationalUnit: string
            WorkContract: string
            ContractType: string
            PayBandDescription: string
            FTE: float
            EmploymentStatus: string
            ReasonforAction: string
            Position: string
            Position1: string
            CostCentre: string
            OrgStartDate: date
            Annualsalary: string
            Curr: string
            SourceSystemID: varchar
            IngestionDate: date
            ValidTo: date
            RowID: string
            IsActive: string
        }

        class LIVE_DIM_DATE {
            date: date
            dim_date_key: int
            day_int: int
            day_name: string
            month_name: string
            month_int: int
            year_int: int
            first_of_month: date
            quarter_int: int
            quarter_name: string
            financial_year: string
            FY_yyyy: int
            MonthYearSortKey: int
            MonthYear: string
            FY: string
        }

        class LIVE_DIM_DATE_LEAVERS {
            date: date
            dim_date_key: int
            day_int: int
            day_name: string
            month_name: string
            month_int: int
            year_int: int
            first_of_month: date
            quarter_int: int
            quarter_name: string
            financial_year: string
            FY_yyyy: int
            MonthYearSortKey: int
            MonthYear: string
            FY: string
        }

        class LIVE_DIM_DATE_STARTED {
            date: date
            dim_date_key: int
            day_int: int
            day_name: string
            month_name: string
            month_int: int
            year_int: int
            first_of_month: date
            quarter_int: int
            quarter_name: string
            financial_year: string
            FY_yyyy: int
            MonthYearSortKey: int
            MonthYear: string
            FY: string
        }
    }

    namespace Curated {

        class VW_HR_MEASURES {
            PersonReportMonthEndKey: string
            PersonKey: string
            FTE: float
            dim_cost_centre_key: string
            dim_personnel_area_key: int
            dim_personnel_subarea_key: int
            dim_organisation_key: int
            Grade: string
            dim_work_contract_key: int
            dim_gender_key: int
            dim_date_key: int
            ReportFlag: string
            dim_started_date_key: int
            dim_leaving_date_key: int
            ReasonForAction: string
            PArea: string
            WorkingHours: float
            LengthOfService: float
            Age: float
            LeaveEntitlementHours: float
            LeaveTakenHours: float
            LeaveRemainingHours: float
            AnnualSalary: float
            Position1: string
            Contract_Type: string
            Directorate: string
        }

        class PBI_HR_MEASURES {
            PersonReportMonthEndKey: string
            PersonKey: string
            FTE: float
            dim_cost_centre_key: string
            dim_personnel_area_key: int
            dim_personnel_subarea_key: int
            dim_organisation_key: int
            Grade: string
            dim_work_contract_key: int
            dim_gender_key: int
            dim_date_key: int
            ReportFlag: string
            dim_started_date_key: int
            dim_leaving_date_key: int
            ReasonForAction: string
            PArea: string
            WorkingHours: float
            LengthOfService: float
            Age: float
            LeaveEntitlementHours: float
            LeaveTakenHours: float
            LeaveRemainingHours: float
            AnnualSalary: float
            Position1: string
            Contract_Type: string
            Directorate: string
        }		
    }

%% Source to Standardised Flow
`sap_hr_leavers.csv` --> `SAP_HR_LEAVERS_MONTHLY`

%% Standardised to Harmonised Flow
`SAP_HR_LEAVERS_MONTHLY` --> `STAGE_SAP_HR_LEAVERS`

%% Harmonised to Harmonised Flow
`STAGE_SAP_HR_LEAVERS` --> `LOAD_SAP_HR_LEAVERS`
`LIVE_DIM_DATE` --> `LIVE_DIM_DATE_LEAVERS`
`LIVE_DIM_DATE` --> `LIVE_DIM_DATE_STARTED`

%% Harmonised to Curated Flow
`LOAD_SAP_HR_LEAVERS` --> `VW_HR_MEASURES`
`HIST_SAP_HR` --> `VW_HR_MEASURES`
`LIVE_DIM_DATE` --> `LOAD_SAP_HR_LEAVERS`
`LIVE_DIM_DATE_LEAVERS` --> `VW_HR_MEASURES`
`LIVE_DIM_DATE_STARTED` --> `VW_HR_MEASURES`
`VW_HR_MEASURES` --> `PBI_HR_MEASURES`