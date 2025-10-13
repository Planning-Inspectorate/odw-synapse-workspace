#### ODW SAP HR Data Model
 
##### entity: SAP_HR_ABSENCE
 
Data model for sap_hr_absence entity showing data flow from source to curated.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class hr_absence_monthly.csv {
            StaffNumber: string
            AbsType: string
            SicknessGroup: string
            StartDate: string
            EndDate: string
            AttendanceorAbsenceType: string
            Days: string
            Hrs: string
            Caldays: string
            WorkScheduleRule: string
            Wkhrs: string
            HrsDay: string
            WkDys: string
            Start: string
            Endtime: string
            AnnualLeaveStart: string
            Illness: string
        }
    }
    
    namespace Standardised {

        class HR_ABSENCE_MONTHLY {
            StaffNumber: string
            AbsType: string
            SicknessGroup: string
            StartDate: string
            EndDate: string
            AttendanceorAbsenceType: string
            Days: string
            Hrs: string
            Caldays: string
            WorkScheduleRule: string
            Wkhrs: string
            HrsDay: string
            WkDys: string
            Start: string
            Endtime: string
            AnnualLeaveStart: string
            Illness: string
            ingested_datetime: date
            expected_from: date
            expected_to: date
        }

        class WORK_SCHEDULES {
            WorkScheduleRule: string
            Mo: string
            Tu: string
            We: string
            Th: string
            Fr: string
            WkHrs: string
            MoTuWeThFr: string
            ingested_datetime: date
            expected_from: date
            expected_to: date
        }

        class LIVE_HOLIDAYS {
            HolidayDate: string
            HolidayDescription: string
            ingested_datetime: date
            expected_from: date
            expected_to: date
        }
    }

    namespace Harmonised {

        class SAP_HR_ABSENCE_ALL {
            StaffNumber: string
            AbsType: string
            SicknessGroup: string
            StartDate: date
            EndDate: date
            AttendanceorAbsenceType: string
            Days: double
            Hrs: double
            Start: date
            Endtime: date
            Caldays: string
            WorkScheduleRule: string
            Wkhrs: double
            HrsDay: double
            WkDys: double
            AnnualLeaveStart: date
            SourceSystemID: string
            IngestionDate: date
            ValidTo: timestamp
            RowID: string
            IsActive: string
        }

        class SAP_HR_WORKSCHEDULERULE {
            WorkScheduleRule: string
            WorkScheduleCode: string
            MoWk1: string
            TuWk1: string
            WeWk1: string
            ThWk1: string
            FrWk1: string
            MoWk2: string
            TuWk2: string
            WeWk2: string
            ThWk2: string
            FrWk2: string
            WkHrsWk1: float
            WkHrsWk2: float
            AvgWkHrs: float
            WSRstart: date
            Currentweek: string
            Lastmodified: date
            IngestionDate: date
        }

        class SAP_HR_FACT_ABSENCE_ALL_TEMP {
            absencedate: date
            absencehours: decimal
            staffnumber: string
            WorkScheduleRule: string
            AbsType: string
            SicknessGroup: string
            AttendanceorAbsenceType: string
            Leave: decimal
            PSGroup: string
            PersonnelArea: string
            PersonnelSubarea: string
            SourceSystemID: string
            IngestionDate: date
            ValidTo: timestamp
            RowID: string
            IsActive: string
        }

        class SAP_HR_FACT_ABSENCE_ALL {
            absencedate: date
            absencehours: decimal
            staffnumber: string
            WorkScheduleRule: string
            AbsType: string
            SicknessGroup: string
            AttendanceorAbsenceType: string
            Leave: decimal
            PSGroup: string
            PersonnelArea: string
            PersonnelSubarea: string
            SourceSystemID: string
            IngestionDate: date
            ValidTo: timestamp
            RowID: string
            IsActive: string
        }

        class SAP_HR_FACT_ABSENCE_SICKNESS {
            sickness_id: int
            StaffNumber: string
            Days: double
            sickness_start: date
            sickness_end: date
            FY: string
            financial_year: string
            calendar_year: string
            SourceSystemID: string
            IngestionDate: date
            ValidFrom: timestamp
            ValidTo: timestamp
            RowID: string
            IsActive: string
            LastUpdated: timestamp
        }

        class HIST_SAP_HR {
            PersNo: string
            Report_MonthEnd_Date: date
            PSGroup: string
            PersonnelArea: string
            PersonnelSubarea: string
            Firstname: string
            Lastname: string
            EmployeeNo: string
            CompanyCode: string
            OrganizationalUnit: string
            WorkContract: string
            PayBandDescription: string
            FTE: float
            EmploymentStatus: string
            Position: string
            Position1: string
            Annualsalary: string
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
            quarter_int: int
            quarter_name: string
            FY: string
            financial_year: string
            FY_yyyy: int
            MonthYearSortKey: int
            MonthYear: string
        }

    }

    namespace Curated {

        class VW_FACT_ABSENCE_ALL {
            absencedate: date
            absencehours: float
            staffnumber: string
            WorkScheduleRule: string
            AbsType: string
            SicknessGroup: string
            AttendanceorAbsenceType: string
            Leave: float
            Leave_ONS: float
            PSGroup: string
            PersonnelArea: string
            PersonnelSubarea: string
            sickness_id: int
            sicknesslength: float
            sicknesslengthtype: string
        }

        class PBI_FACT_ABSENCE_ALL {
            absencedate: date
            absencehours: float
            staffnumber: string
            WorkScheduleRule: string
            AbsType: string
            SicknessGroup: string
            AttendanceorAbsenceType: string
            Leave: float
            Leave_ONS: float
            PSGroup: string
            PersonnelArea: string
            PersonnelSubarea: string
            sickness_id: int
            sicknesslength: float
            sicknesslengthtype: string
        }
    }

%% Source to Standardised Flow
`hr_absence_monthly.csv` --> `HR_ABSENCE_MONTHLY`

%% Standardised to Harmonised Flow
`HR_ABSENCE_MONTHLY` --> `SAP_HR_ABSENCE_ALL`
`WORK_SCHEDULES` --> `SAP_HR_WORKSCHEDULERULE`

%% Harmonised to Harmonised Flow
`SAP_HR_ABSENCE_ALL` --> `SAP_HR_FACT_ABSENCE_ALL_TEMP`
`SAP_HR_WORKSCHEDULERULE` --> `SAP_HR_FACT_ABSENCE_ALL_TEMP`
`SAP_HR_FACT_ABSENCE_ALL_TEMP` --> `SAP_HR_FACT_ABSENCE_ALL`
`HIST_SAP_HR` --> `SAP_HR_FACT_ABSENCE_ALL`
`LIVE_HOLIDAYS` --> `SAP_HR_FACT_ABSENCE_ALL`
`SAP_HR_ABSENCE_ALL` --> `SAP_HR_FACT_ABSENCE_SICKNESS`
`LIVE_HOLIDAYS` --> `SAP_HR_FACT_ABSENCE_SICKNESS`
`LIVE_DIM_DATE` --> `SAP_HR_FACT_ABSENCE_SICKNESS`

%% Harmonised to Curated Flow
`SAP_HR_FACT_ABSENCE_ALL` --> `VW_FACT_ABSENCE_ALL`
`SAP_HR_FACT_ABSENCE_SICKNESS` --> `VW_FACT_ABSENCE_ALL`
`WORK_SCHEDULES` --> `VW_FACT_ABSENCE_ALL`

%% Curated to Curated Flow
`VW_FACT_ABSENCE_ALL` --> `PBI_FACT_ABSENCE_ALL`