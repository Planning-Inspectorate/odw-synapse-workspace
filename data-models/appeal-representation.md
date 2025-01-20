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
        class Standardised.sb_appeals_representation {
            representationId: string
        }
    }

    namespace Harmonised {
        class Harmonised.sb_appeals_representation {
            representationId: string
        }
    }

    namespace Curated {
        class Curated.sb_appeals_representation {
            representationId: string
        }
    }

`appeal-representation` --> `Standardised.sb_appeals_representation`
`Standardised.sb_appeals_representation` --> `Harmonised.sb_appeals_representation`
`Harmonised.sb_appeals_representation` --> `Curated.sb_appeals_representation`


```
