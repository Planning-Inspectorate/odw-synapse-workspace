#### ODW Data Model
 
##### entity: appeal-event-estimate
 
Data model for appeal-event-estimate entity showing data flow from source to curated.
 
```mermaid
 
classDiagram
    direction LR
 
    namespace Sources {
        class appeal-event-estimate {
            Id: int
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
        class appeal_event_estimate_cu {
            Id: int
        }
    }
 
appeal-event-estimate --> sb_appeal_event_estimate_std
sb_appeal_event_estimate_std --> sb_appeal_event_estimate_hrm
sb_appeal_event_estimate_hrm --> appeal_event_estimate_cu
