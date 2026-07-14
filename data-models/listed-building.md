##### ODW Data Model

###### entity: listed-building

Data model for listed-building entity showing data flow from the Gov.uk Planning Data API source through to curated, and publication to the Appeals Back Office Service Bus.

```mermaid
classDiagram
    direction LR

    namespace Sources {
        class GovUK_ListedBuilding_API {
            entity: int
            reference: string
        }

        class GovUK_ListedBuildingOutline_API {
            entity: int
            reference: string
        }
    }

    namespace Standardised {
        class listed_building_std {
            entity: int
        }

        class listed_building_outline_std {
            entity: int
        }
    }

    namespace Harmonised {
        class listed_building_hrm {
            entity: int
        }
    }

    namespace Curated {
        class listed_building_cur {
            entity: long
            reference: string
            name: string
            listedBuildingGrade: string
        }
    }

    namespace Appeal_Service_Bus {
        class listed_building_topic {
            entity: long
        }
    }

    `GovUK_ListedBuilding_API` --> `listed_building_std`
    `GovUK_ListedBuildingOutline_API` --> `listed_building_outline_std`
    `listed_building_std` --> `listed_building_hrm`
    `listed_building_hrm` --> `listed_building_cur`
    `listed_building_cur` --> `listed_building_topic`
```

Tables and views

- Standardised
  - odw_standardised_db.listed_building
  - odw_standardised_db.listed_building_outline

- Harmonised
  - odw_harmonised_db.listed_building

- Curated
  - odw_curated_db.listed_building

- Service Bus
  - listed_building_topic

Orchestration and lineage

- Notebooks and SQL scripts
  - py_listed_building_raw_to_std (loads listed_building.json into odw_standardised_db.listed_building)
  - py_listed_building_outline_raw_to_std (loads listed building outline data into odw_standardised_db.listed_building_outline)
  - listed_building (builds odw_harmonised_db.listed_building from standardised listed building data)
  - listed_building_curated (builds odw_curated_db.listed_building from harmonised listed building data)
  - listed_building_topic (publishes curated listed building data to the Appeals Back Office Service Bus)
