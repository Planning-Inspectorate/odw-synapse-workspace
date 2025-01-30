#### ODW Data Model

##### entity: appeal-representation

Data model for appeal-representation entity showing Service Bus data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {
        class appeal-representation {
            representationId: string
        }
    }
    
    namespace Standardised {
        class Standardised.sb_appeal_representation {
            representationId: string
        }
    }

    namespace Harmonised {
        class Harmonised.sb_appeal_representation {
            representationId: string
        }
    }

    namespace Curated {
        class Curated.appeal_representation {
            representationId: string
        }
    }

`appeal-representation` --> `Standardised.sb_appeal_representation`
`Standardised.sb_appeal_representation` --> `Harmonised.sb_appeal_representation`
`Harmonised.sb_appeal_representation` --> `Curated.appeal_representation`


```
