#### ODW Data Model
 
##### entity: appeal-event-estimate
 
Data model for appeal-event-estimate entity showing data flow from source to curated.
 
```mermaid
classDiagram

    direction LR

    namespace Sources {

        class appeal_event_estimate_sb_src {
            Id: int
        }
    }

    namespace Raw {

        class appeal_event_estimate_sb_raw {
            odw-raw/ServiceBus/appeal-event-estimate/
        }
    }

    namespace Standardised {

        class sb_appeal_event_estimate_std {
            Id: int
        }
    }

    namespace Harmonised {

        class sb_appeal_event_estimate_hrm {
            Id: int
        }
    }

    namespace Curated {

        class appeal_event_estimate {
            Id: int
        }
    }

    namespace MiPINS {

        class appeal_event_estimate_curated_mipins {
            Id: int
        }
    }

    appeal_event_estimate_sb_src --> appeal_event_estimate_sb_raw

    appeal_event_estimate_sb_raw --> sb_appeal_event_estimate_std

    sb_appeal_event_estimate_std --> sb_appeal_event_estimate_hrm

    sb_appeal_event_estimate_hrm --> appeal_event_estimate

    sb_appeal_event_estimate_hrm --> appeal_event_estimate_curated_mipins
```


### Tables and views

- Raw (Azure Data Lake odw-raw)
  - odw-raw/ServiceBus/appeal-event-estimate/ (service bus messages landed in the raw layer)

- Standardised
  - odw_standardised_db.sb_appeal_event_estimate

- Harmonised
  - odw_harmonised_db.sb_appeal_event_estimate

- Curated
  - odw_curated_db.appeal_event_estimate

- MiPINS
  - odw_curated_db.appeal_event_estimate_curated_mipins

- Views
  - odw_curated_db.vw_appeal_event_estimate_curated_mipins

### Orchestration and lineage

- Pipelines

  - workspace/pipeline/pln_entity_appeal_event_estimate.json
    - Executes:
      - appeal_event_estimate notebook

  - workspace/pipeline/pln_copy_appeal_event_estimate_curated_mipins.json
    - Copies appeal_event_estimate_curated_mipins dataset to MiPINS SQL destination

- Notebooks

  - workspace/notebook/appeal_event_estimate.json
    - Reads:
      - odw_harmonised_db.sb_appeal_event_estimate
    - Filters:
      - IsActive = 'Y'
    - Writes:
      - odw_curated_db.appeal_event_estimate

  - workspace/notebook/appeal_event_estimate_curated_mipins.json
    - Reads:
      - odw_harmonised_db.sb_appeal_event_estimate
    - Creates view:
      - odw_curated_db.vw_appeal_event_estimate_curated_mipins
    - Writes:
      - odw_curated_db.appeal_event_estimate_curated_mipins
