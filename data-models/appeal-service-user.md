#### ODW Data Model

##### entity: appeal-service-user

Data model for appeal-service-user entity showing data flow from source to curated.

```mermaid
classDiagram

    direction LR

    namespace Sources {

        class horizon_odw_vw_serviceuser {
            Horizon_ODW_vw_ServiceUser
        }

        class service_bus {
            Service Bus
        }
    }

    namespace Raw {

        class case_involvement_raw {
            CaseInvolvement.csv
        }

        class service_user_raw {
            service-user
        }
    }

    namespace Standardised {

        class horizon_case_involvement {
            horizon_case_involvement
        }
        
        class horizon_nsip_data {
            horizon_nsip_data
        }

        class horizon_nsip_relevant_representation {
            horizon_nsip_relevant_representation
        }
        
        class sb_service_user {
            sb_service_user
        }
    }

    namespace Harmonised {

        class service_user {
            TEMP_PK
        }
    }

    namespace Curated {

        class appeal_service_user {
            appeal_service_user
        }
    }

    namespace MiPINS {

        class appeal_service_user_curated_mipins {
            appeal_service_user_curated_mipins
        }
    }

    horizon_odw_vw_serviceuser --> case_involvement_raw
    case_involvement_raw --> horizon_case_involvement
    service_bus --> service_user_raw
    service_user_raw --> sb_service_user
    horizon_case_involvement --> service_user
    horizon_nsip_data --> service_user
    horizon_nsip_relevant_representation --> service_user
    sb_service_user --> service_user
    service_user --> appeal_service_user
    appeal_service_user --> appeal_service_user_curated_mipins
```

### Tables and views

- Raw
  - odw-raw/Horizon/CaseInvolvement.csv
  - odw-raw/ServiceBus/service-user

- Standardised
  - odw_standardised_db.horizon_case_involvement
  - odw_standardised_db.horizon_nsip_data
  - odw_standardised_db.horizon_nsip_relevant_representation
  - odw_standardised_db.sb_service_user

- Harmonised
  - odw_harmonised_db.service_user

- Curated
  - odw_curated_db.appeal_service_user

- MiPINS
  - odw_curated_db.appeal_service_user_curated_mipins

### Orchestration and lineage

- Pipelines
  - 0_Horizon_Case_Involvement
  - pln_horizon_case_involvement
  - pln_service_bus_service_user
  - pln_service_user_main
  - pln_curated
  - pln_copy_appeal_service_user_curated_mipins

- Notebooks
  - py_sb_horizon_harmonised_service_user
  - appeal_service_user
  - appeal_service_user_curated_mipins


