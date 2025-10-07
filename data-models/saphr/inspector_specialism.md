#### ODW SAPHR Data Model

##### entity: Inspector_Specialisms

Data model for Inspector_Specialisms data flow from source to harmonised.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class Inspector_specialisms.csv {
            StaffNumber: string
            Firstname: string
            Lastname: string
            QualificationName: string
            Proficien: string
        }

    }
    
    namespace Standardised {

        class INSPECTOR_SPECIALISMS_MONTHLY {
            ingested_datetime: date
            expected_from: date
            expected_to: date
            StaffNumber: string
            Firstname: string
            Lastname: string
            QualificationName: string
            Proficien: string
        }

    }

    namespace Harmonised {

        class TRANSFORM_INSPECTOR_SPECIALISMS {
            StaffNumber: string
            Firstname: string
            Lastname: string
            QualificationName: string
            Proficien: string
            SourceSystemID: string
            IngestionDate: date
            ValidTo: date
            RowID: string
            IsActive: string
        }

        class SAP_HR_INSPECTOR_SPECIALISMS {
            StaffNumber: string
            Firstname: string
            Lastname: string
            QualificationName: string
            Proficien: string
            SourceSystemID: string
            IngestionDate: date
            ValidFrom: date
            ValidTo: date
            LastUpdated: date
            Current: int
            RowID: string
            IsActive: string
        }
    
    }

%% Source to Standardised Flow
`Inspector_specialisms.csv` --> `INSPECTOR_SPECIALISMS_MONTHLY`

%% Standardised to Harmonised Flow
`INSPECTOR_SPECIALISMS_MONTHLY` --> `TRANSFORM_INSPECTOR_SPECIALISMS`

%% Harmonised to Harmonised Flow
`TRANSFORM_INSPECTOR_SPECIALISMS` --> `SAP_HR_INSPECTOR_SPECIALISMS`