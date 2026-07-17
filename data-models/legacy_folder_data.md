##### ODW Data Model

###### entity: legacy_folder_data

Data model for legacy_folder_data entity showing data flow from source to curated.

```mermaid
classDiagram
    direction LR

    namespace Sources {
        class HorizonFolder_csv_src {
            ID: string
        }
    }

    namespace Standardised {
        class horizon_folder {
            ID: string
        }
    }

    namespace Harmonised {
        class main_sourcesystem_fact {
            SourceSystemID: string
        }

        class horizon_folder_hrm {
            HorizonFolderID: string
            ID: string
            CaseReference: string
            ParentFolderID: string
            SourceSystemID: string
        }
    }

    namespace Curated {
        class legacy_folder_data {
            id: int
            caseReference: string
            parentFolderId: int
            caseStage: string
        }
    }

    `HorizonFolder_csv_src` --> `horizon_folder`
    `horizon_folder` --> `horizon_folder_hrm`
    `main_sourcesystem_fact` --> `horizon_folder_hrm`
    `horizon_folder_hrm` --> `legacy_folder_data`
```

Tables and views

- Standardised
  - odw_standardised_db.horizon_folder

- Harmonised
  - odw_harmonised_db.main_sourcesystem_fact
  - odw_harmonised_db.horizon_folder

- Curated
  - odw_curated_db.legacy_folder_data

Orchestration and lineage

- Notebooks and SQL scripts
  - py_horizon_raw_to_std (loads HorizonFolder.csv into odw_standardised_db.horizon_folder)
  - horizon_folder (builds odw_harmonised_db.horizon_folder from odw_standardised_db.horizon_folder using hash-based change detection and SCD Type 2 processing)
  - legacy_folder_data (builds odw_curated_db.legacy_folder_data from odw_harmonised_db.horizon_folder)
