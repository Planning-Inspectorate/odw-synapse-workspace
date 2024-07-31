#### ODW Data Model

##### entity: appeal-event

Data model for appeal-event entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class Horizon_vw_Event {
            caseNumber: varchar
            ,eventId: string
        }

        class Service_bus_appeal_event  {
            eventId: string
        }
    }

    namespace Standardised {

        class Horizon_appeals_event {
            caseNumber: varchar
            ,eventId: string
        }

        class sb_appeal_event  {
            eventId: string
        }
    }

    namespace Harmonised {

        class sb_appeals_event  {
            eventId: string
        }
    }

    namespace Curated {

        class appeal_event {
            eventId: string
        }
    }

`Horizon_vw_Event` --> `Horizon_appeals_event`
`Service_bus_appeal_event` --> `sb_appeal_event`

`Horizon_appeals_event` --> `sb_appeals_event` 
`sb_appeal_event` --> `sb_appeals_event`



`sb_appeals_event` --> `appeal_event`

```