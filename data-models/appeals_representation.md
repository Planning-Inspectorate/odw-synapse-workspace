#### ODW Data Model
 
##### entity: appeals-representation
 
Data model for appeals-representation entity showing data flow from source to curated.
 
```mermaid
 
classDiagram
    direction LR
 
    namespace Sources {
        class appeals_representation {
            representationId: int
        }
    }
    namespace Standardised {
        class sb_appeals_representation_std {
            representationId: int
        }
    }
 
    namespace Harmonised {
        class sb_appeals_representation_hrm {
            representationId: int
        }
    }
 
    namespace Curated {
        class appeals_representation_cu {
            representationId: int
        }
    }
 
appeals_representation --> sb_appeals_representation_std
sb_appeals_representation_std --> sb_appeals_representation_hrm
sb_appeals_representation_hrm --> appeals_representation_cu
