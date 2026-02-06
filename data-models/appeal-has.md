#### ODW Data Model

##### entity: appeal-has

Data model for appeal-has entity showing Service Bus data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class ServiceBus_pins-sb-appeals-bo-dev-appeal-has {
            caseId: bigint
        }

        class Horizon_Views {
            caseRef: str
            casenodeId: str
            appealrefNo: str
            Id: str
            advertAttrKey: str
            id: int
        }
    }
    
    namespace Standardised {

        class sb_appeal_has_std {
            caseId: bigint
        }

        class horizon_appeal_has {
            caseRef: str
        }
    }

    namespace Harmonised {

        class sb_appeal_has_hrm {
            caseId: bigint
        }

        class appeal_has_stg {
            AppealsHasID: bigint
        }

        class appeal_has_hrm {
            caseRef: varchar
        }

    }

    namespace Curated {

        class appeals_has_cur {
            caseId: long
        }

    }

`ServiceBus_pins-sb-appeals-bo-dev-appeal-has` --> `sb_appeal_has_std`
`Horizon_Views` --> `horizon_appeal_has`
`sb_appeal_has_std` --> `sb_appeal_has_hrm`
`horizon_appeal_has` --> `appeal_has_stg`
`sb_appeal_has_hrm` --> `appeal_has_stg`
`appeal_has_stg` --> `appeal_has_hrm`
`appeal_has_hrm` --> `appeals_has_cur`


```
