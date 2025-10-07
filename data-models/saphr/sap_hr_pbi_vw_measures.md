#### ODW SAP HR Data Model
 
##### entity: PBI HR Measures PowerBI Semantic Model
 
PowerBI Semantic Model for PBI HR Measures entity showing data flow from source to curated.

```mermaid
classDiagram

    direction LR
    
    namespace Standardised {

        class SAP_FINANCE_DIM_COSTCENTRE {
            CostCentre: string
            CostCentreDesc: string
            Branch: string
            Directorate: string
            ingested_datetime: date
        }

        class SAP_HR_HIST {
            PersNo: string
            Report_MonthEnd_Date: date
            Firstname: string
            Lastname: string
            EmployeeNo: string
            PersonnelArea: string
            OrganizationalUnit: string
            WorkContract: string
            PSgroup: string
            FTE: float
            GenderKey: string
            CostCentre: string
            Annualsalary: string
        }

        class SAP_HR_LEAVERS {
            PersNo: string
            Leaving: date
            PSgroup: string
            OrganizationalUnit: string
            PersonnelArea: string
            Annualsalary: float
        }

        class SAP_FINANCE_COST_CENTRE {
            CostCentre: string
            CostCentreDesc: string
            Directorate: string
        }

        class DIM_DATE {
            date: date
            dim_date_key: int
            financial_year: string
        }		
    }

    namespace Harmonised {

        class HIST_SAP_HR {
            PersNo: string
            Report_MonthEnd_Date: date
            Firstname: string
            Lastname: string
            EmployeeNo: string
            PersonnelArea: string
            OrganizationalUnit: string
            WorkContract: string
            PSgroup: string
            FTE: float
            GenderKey: string
            CostCentre: string
            Annualsalary: string
            IngestionDate: date
            ValidTo: date
            IsActive: string
        }

        class LOAD_SAP_HR_LEAVERS {
            PersNo: string
            OrgStartDate: date
            Leaving: date
            PSgroup: string
            OrganizationalUnit: string
            PersonnelArea: string
            Annualsalary: float
            IngestionDate: date
            ValidTo: date
            IsActive: string
        }

        class LIVE_DIM_DATE {
            date: date
            dim_date_key: int
            financial_year: string
            FY: string
        }

        class LIVE_DIM_DATE_LEAVERS {
            date: date
            dim_date_key: int
            FY: string
        }

        class LIVE_DIM_DATE_STARTED {
            date: date
            dim_date_key: int
            FY: string
        }
    }

    namespace Curated {

        class VW_DIM_HR_PERSONNEL_AREA {
            dim_personnel_area_key: int
            PersonnelAreaCode: string
            PersonnelArea: string
        }

        class VW_DIM_HR_PERSONNEL_SUBAREA {
            dim_personnel_subarea_key: int
            PersonnelSubAreaCode: string
            PersonnelSubArea: string
        }

        class VW_DIM_HR_ORGANISATION_UNIT {
            dim_organisation_key: int
            OrganisationUnitCode: string
            OrganisationUnitDesc: string
        }

        class PBI_DIM_HR_GRADE {
            Grade: string
            GradeDescription: string
            dim_grade_key: int
        }

        class VW_DIM_HR_WORK_CONTRACT {
            dim_work_contract_key: int
            WorkContractCode: string
            WorkContract: string
        }

        class VW_DIM_HR_GENDER {
            dim_gender_key: int
            gender: string
        }

        class VW_HR_MEASURES {
            PersonReportMonthEndKey: string
            PersonKey: string
            FTE: float
            dim_cost_centre_key: string
            dim_personnel_area_key: int
            dim_organisation_key: int
            Grade: string
            dim_date_key: int
            ReportFlag: string
            WorkingHours: float
            AnnualSalary: float
        }

        class PBI_HR_MEASURES {
            PersonReportMonthEndKey: string
            PersonKey: string
            FTE: float
            dim_date_key: int
            AnnualSalary: float
        }
    }


%% Standardised to Harmonised Flow
`DIM_DATE` --> `LIVE_DIM_DATE`
`DIM_DATE` --> `LIVE_DIM_DATE_LEAVERS`
`DIM_DATE` --> `LIVE_DIM_DATE_STARTED`
`SAP_HR_LEAVERS` --> `LOAD_SAP_HR_LEAVERS` 

%% Harmonised to Curated Flow
`HIST_SAP_HR` --> `VW_DIM_HR_PERSONNEL_AREA`
`HIST_SAP_HR` --> `VW_DIM_HR_PERSONNEL_SUBAREA`
`HIST_SAP_HR` --> `VW_DIM_HR_ORGANISATION_UNIT`
`HIST_SAP_HR` --> `PBI_DIM_HR_GRADE`
`HIST_SAP_HR` --> `VW_DIM_HR_WORK_CONTRACT`
`HIST_SAP_HR` --> `VW_DIM_HR_GENDER`
`HIST_SAP_HR` --> `VW_HR_MEASURES`
`LOAD_SAP_HR_LEAVERS` --> `VW_HR_MEASURES`
`SAP_FINANCE_DIM_COSTCENTRE` --> `VW_HR_MEASURES`
`LIVE_DIM_DATE` --> `VW_HR_MEASURES`
`LIVE_DIM_DATE_STARTED` --> `VW_HR_MEASURES`
`LIVE_DIM_DATE_LEAVERS` --> `VW_HR_MEASURES`

%% Curated Dimension to Fact Flow
`VW_DIM_HR_PERSONNEL_AREA` --> `VW_HR_MEASURES`
`VW_DIM_HR_PERSONNEL_SUBAREA` --> `VW_HR_MEASURES`
`VW_DIM_HR_ORGANISATION_UNIT` --> `VW_HR_MEASURES`
`PBI_DIM_HR_GRADE` --> `VW_HR_MEASURES`
`VW_DIM_HR_WORK_CONTRACT` --> `VW_HR_MEASURES`
`VW_DIM_HR_GENDER` --> `VW_HR_MEASURES`
`VW_HR_MEASURES` --> `PBI_HR_MEASURES`
