#### ODW Data Model

##### entity: nsip-project

Data model for nsip-project entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class HZN_NSIP_Data_Copy {
            CaseID:int
        }

        class nsip-project {
            CaseID
        }


    }
    
    namespace Standardised {

        class horizon_nsip_data {
             casenodeid:int
        }

        class nsip_project {
            CaseID
        }

    }

    namespace Harmonised {


        class casework_nsip_data_dim {
            horizon_nsip_data
            nsip_project
        }

    }

    namespace Curated {

        class nsip_data {
            CaseID: int
        }
    }


`HZN_NSIP_Data_Copy` --> `horizon_nsip_data`
`nsip-project` -->`nsip_project`

`horizon_nsip_data` --> `casework_nsip_data_dim`

`nsip_project`--> `casework_nsip_data_dim`

`casework_nsip_data_dim` --> `nsip_data`

```