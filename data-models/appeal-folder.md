#### ODW Data Model

##### entity: appeal-folder

Data model for appeal-folder entity showing data flow from source to curated.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class horizon_appeals_folder_src {
            Horizon_ODW_vw_FolderEntity
        }
    }

    namespace Raw {

        class horizon_appeals_folder_raw {
            HorizonAppealsFolder.csv
        }
    }

    namespace Standardised {

        class horizon_appeals_folder_std {
            Id: int
        }
    }

    namespace Harmonised {

        class appeals_folder_hrm {
            HorizonAppealFolderId: int
        }
    }

    namespace Curated {

        class appeal_folder {
            Id: int
        }
    }

    horizon_appeals_folder_src --> horizon_appeals_folder_raw

    horizon_appeals_folder_raw --> horizon_appeals_folder_std

    horizon_appeals_folder_std --> appeals_folder_hrm

    appeals_folder_hrm --> appeal_folder
```

### Tables and views

- Raw
  - odw-raw/Horizon/HorizonAppealsFolder.csv

- Standardised
  - odw_standardised_db.horizon_appeals_folder

- Harmonised
  - odw_harmonised_db.appeals_folder

- Curated
  - odw_curated_db.appeal_folder

- Views
  - odw_curated_db.vw_appeal_folder

### Orchestration and lineage

- Pipelines
  - 0_Raw_Horizon_Appeals_Folder
  - pln_horizon_appeals_folder
  - pln_curated

- Notebooks
  - appeals_folder
  - appeals_folder_curated



