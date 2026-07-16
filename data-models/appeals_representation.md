#### ODW Data Model
 
##### entity: appeals-representation
 
Data model for appeals-representation entity showing data flow from source to curated.
 
```mermaid
classDiagram

    direction LR

    namespace Sources {

        class service_bus {
            Service_Bus
        }
    }

    namespace Raw {

        class appeal_representation_raw {
            appeal-representation
        }
    }

    namespace Standardised {

        class sb_appeal_representation_std {
            sb_appeal_representation
        }
    }

    namespace Harmonised {

        class sb_appeal_representation_hrm {
            sb_appeal_representation
        }
    }

    namespace Curated {

        class appeal_representation {
            appeal_representation
        }
    }

    service_bus --> appeal_representation_raw

    appeal_representation_raw --> sb_appeal_representation_std

    sb_appeal_representation_std --> sb_appeal_representation_hrm

    sb_appeal_representation_hrm --> appeal_representation
```


### Tables and views

- Raw
  - odw-raw/ServiceBus/appeal-representation

- Standardised
  - odw_standardised_db.sb_appeal_representation

- Harmonised
  - odw_harmonised_db.sb_appeal_representation

- Curated
  - odw_curated_db.appeal_representation
 
### Orchestration and lineage

- Pipelines
  - pln_curated

- Notebooks
  - py_sb_std_to_hrm
  - appeal_representation
