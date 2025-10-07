#### ODW SAP HR Data Model
 
##### entity: SAP_HR_Protected(PC)
 
Data model for SAP_HR_Protected(PC) entity showing data flow from source to curated.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class SAP_Protected.csv {
            RefNo: string
            EthnicOrigin: string
            ReligiousDenominationKey: string
            SxO: string
            Grade: string
            DisabilityText: string
            DisabilityCodeDescription: string
            Disabled: string
            Ethnicity: string
            Religion: string
            SexualOrientation: string
            Report_MonthEnd_Date: string
        }
    }
    
    namespace Standardised {

        class SAP_PROTECTED_MONTHLY {
            RefNo: string
            EthnicOrigin: string
            ReligiousDenominationKey: string
            SxO: string
            Grade: string
            DisabilityText: string
            DisabilityCodeDescription: string
            Disabled: string
            Ethnicity: string
            Religion: string
            SexualOrientation: string
            Report_MonthEnd_Date: string
            ingested_datetime: date
            expected_from: date
            expected_to: date
        }
    }

    namespace Harmonised {

        class SAP_HR_PC {
            RefNo: string
            EthnicOrigin: string
            ReligiousDenominationKey: string
            SxO: string
            Grade: string
            DisabilityText: string
            Report_MonthEnd_Date: date
            SourceSystemID: string
            IngestionDate: date
            ValidTo: date
            RowID: string
            IsActive: string
        }

        class SAP_HR_PROTECTED_DATA {
            RefNo: string
            EthnicOrigin: string
            ReligiousDenominationKey: string
            SxO: string
            Grade: string
            DisabilityText: string
            Report_MonthEnd_Date: date
            SourceSystemID: string
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
            week_int: int
            iso_week_int: int
            day_of_week_int: int
            quarter_int: int
            quarter_name: string
            first_of_year: string
            week_ending_date: string
            financial_year: string
            FY_yyyy: int
            week_ending_year: int
            week_ending_quarter_int: int
            week_ending_quarter_name: string
            week_ending_month_int: int
            week_ending_month_name: string
            week_ending_day: int
            MonthYearSortKey: int
            MonthYear: string
            FY: string
            FY_Latest: string
            FY_MonthYearLatest: string
            FY_Quarter: string
            FY_Latest_SortKey: int
            week_starting_date: string
        }
    }

    namespace Curated {

        class VW_HR_PROTECTEDDATA {
            PC_Date: date
            PC_Month: string
            month_int: int
            PC_Month_Latest: string
            MonthYearLatest_SortKey: bigint
            PC_Year: int
            PC_FY: int
            PC_FY_Latest: string
            FY_Latest_SortKey: int
            Ref_Number: string
            DisabilityText: string
            Disabled: string
            Disability_group: string
            EthnicOrigin: string
            Ethnicity: string
            Ethnicity_group: string
            ReligiousDenominationKey: string
            Religion: string
            Religion_group: string
            SxO: string
            Sexual_Orientation: string
            Sexual_Orientation_group: string
            PC_Headcount: int
            Data_Completeness: string
        }

        class PBI_HR_PROTECTEDDATA {
            PC_Date: date
            PC_Month: string
            month_int: int
            PC_Month_Latest: string
            MonthYearLatest_SortKey: bigint
            PC_Year: int
            PC_FY: int
            PC_FY_Latest: string
            FY_Latest_SortKey: int
            Ref_Number: string
            DisabilityText: string
            Disabled: string
            Disability_group: string
            EthnicOrigin: string
            Ethnicity: string
            Ethnicity_group: string
            ReligiousDenominationKey: string
            Religion: string
            Religion_group: string
            SxO: string
            Sexual_Orientation: string
            Sexual_Orientation_group: string
            PC_Headcount: int
            Data_Completeness: string
        }
    }

%% Source to Standardised Flow
`SAP_Protected.csv` --> `SAP_PROTECTED_MONTHLY`

%% Standardised to Harmonised Flow
`SAP_PROTECTED_MONTHLY` --> `SAP_HR_PC`

%% Harmonised to Harmonised Flow
`SAP_HR_PC` --> `SAP_HR_PROTECTED_DATA`

%% Harmonised to Curated Flow
`SAP_HR_PROTECTED_DATA` --> `VW_HR_PROTECTEDDATA`
`LIVE_DIM_DATE` --> `VW_HR_PROTECTEDDATA`

%% Curated to Curated Flow
`VW_HR_PROTECTEDDATA` --> `PBI_HR_PROTECTEDDATA`