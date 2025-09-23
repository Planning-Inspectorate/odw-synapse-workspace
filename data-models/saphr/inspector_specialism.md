classDiagram

    direction LR

    namespace Sources {

        class sap_hr_specialisms.csv {
            StaffNumber: varchar
            Firstname: varchar
            Lastname: varchar
            QualificationName: varchar
            Proficien: varchar
        }

    }
    
    namespace Standardised {

        class INSPECTOR_SPECIALISMS_MONTHLY {
            ingested_datetime: date
            expected_from: date
            expected_to: date
            StaffNumber: varchar
            Firstname: varchar
            Lastname: varchar
            QualificationName: varchar
            Proficien: varchar
        }

    }

    namespace Harmonised {

        class TRANSFORM_INSPECTOR_SPECIALISMS {
            StaffNumber: varchar
            Firstname: varchar
            Lastname: varchar
            QualificationName: varchar
            Proficien: varchar
            SourceSystemID: varchar
            IngestionDate: date
            ValidTo: date
            RowID: varchar
            IsActive: varchar
        }

        class SAP_HR_INSPECTOR_SPECIALISMS {
            StaffNumber: varchar
            Firstname: varchar
            Lastname: varchar
            QualificationName: varchar
            Proficien: varchar
            SourceSystemID: varchar
            IngestionDate: date
            ValidFrom: date
            ValidTo: date
            LastUpdated: date
            Current: int
            RowID: varchar
            IsActive: varchar
        }
    
    }

%% Source to Standardised Flow
sap_hr_specialisms.csv --> INSPECTOR_SPECIALISMS_MONTHLY

%% Standardised to Harmonised Flow
INSPECTOR_SPECIALISMS_MONTHLY --> TRANSFORM_INSPECTOR_SPECIALISMS

%% Harmonised to Harmonised Flow
TRANSFORM_INSPECTOR_SPECIALISMS --> SAP_HR_INSPECTOR_SPECIALISMS