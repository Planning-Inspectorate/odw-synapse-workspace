#### ODW Data Model

##### entity: nsip-subscription

Data model for nsip-subscription entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class nsip_subscription_sb_src {
            ODT Service Bus
            entity: nsip-subscription
        }
    }

    namespace Raw {

        class nsip_subscription_raw {
            odw-raw/ServiceBus
            nsip-subscription/
        }
    }

    namespace Standardised {

        class sb_nsip_subscription {
            odw_standardised_db
            subscriptionId: string
        }
    }

    namespace Harmonised {

        class sb_nsip_subscription_hrm {
            odw_harmonised_db
            sb_nsip_subscription
            subscriptionId: string
        }
    }

    namespace Curated {

        class nsip_subscription_cur {
            odw_curated_db
            nsip_subscription
            subscriptionId: string
        }
    }

`nsip_subscription_sb_src` --> `nsip_subscription_raw` : pln_trigger_function_app

`nsip_subscription_raw` --> `sb_nsip_subscription` : py_raw_to_std + py_service_bus_json_to_csv

`sb_nsip_subscription` --> `sb_nsip_subscription_hrm` : py_std_to_hrm

`sb_nsip_subscription_hrm` --> `nsip_subscription_cur` : nsip_subscription notebook

```

Tables and views
- Raw (Azure Data Lake odw-raw)
  - odw-raw/ServiceBus/nsip-subscription/ (service bus messages landed by function app)
- Standardised
  - odw_standardised_db.sb_nsip_subscription (service bus messages, JSON converted to CSV)
- Harmonised
  - odw_harmonised_db.sb_nsip_subscription (harmonised staging table — output of py_std_to_hrm; no separate merge step)
- Curated
  - odw_curated_db.nsip_subscription (external curated table)
- MiPINS
  - No MiPINS curated step for this entity


Orchestration and lineage
- Pipelines
  - workspace/pipeline/pln_service_bus_nsip_subscription.json (all steps in one pipeline)
    - Src to Raw: pln_trigger_function_app → odw-raw/ServiceBus/nsip-subscription/
    - Raw to Std: py_raw_to_std + py_service_bus_json_to_csv → odw_standardised_db.sb_nsip_subscription
    - Std to Hrm: py_std_to_hrm → odw_harmonised_db.sb_nsip_subscription
    - Harmonised to Curated: nsip_subscription notebook → odw_curated_db.nsip_subscription
- Notebooks
  - workspace/notebook/nsip_subscription.json
    - Reads: odw_harmonised_db.sb_nsip_subscription
    - Writes: odw_curated_db.nsip_subscription
