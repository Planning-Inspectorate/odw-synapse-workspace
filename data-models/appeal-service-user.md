#### ODW Data Model

##### entity: appeal-service-user

Data model for appeal-service-user entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Standardised {

        class horizon_case_involvement {
            id: string
        }

        class horizon_nsip_data {
            id: string
        }

        class horizon_nsip_relevant_representation {
            id: string
        }
    }

    namespace Harmonised {

        class sb_service_user {
            id: string
        }

        class service_user {
            id: string
        }
    }

    namespace Curated {

        class appeal_service_user {
            id: string
        }

        class appeal_service_user_curated_mipins {
            id: string
        }
    }

horizon_case_involvement --> service_user

horizon_nsip_data --> service_user

horizon_nsip_relevant_representation --> service_user

sb_service_user --> service_user

service_user --> appeal_service_user

sb_service_user --> appeal_service_user_curated_mipins

service_user --> appeal_service_user_curated_mipins
