#### ODW Data Model

##### entity: nsip-s51-advice

Data model for nsip-s51-advice entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class Horizon_NSIP_Advice {
            CaseID:int
        }

        class nsip-s51-advice{
            CaseNodeID:int
        }


    }
    
    namespace Standardised {

        class horizon_nsip_advice {
             CaseID:int
        }

        class nsip_s51_advice {
            
        }

    }

    namespace Harmonised {


        class casework_nsip_advice_dim {
            horizon_nsip_advice
            nsip_s51_advice 
        }

    }

    namespace Curated {

        class nsip_s51_advice {
            NSIPAdviceID: int
        }
    }


`Horizon_NSIP_Advice` --> `horizon_nsip_advice`
`nsip-s51-advice` -->` nsip_s51_advice `

`horizon_nsip_advice` --> `casework_nsip_advice_dim`

` nsip_s51_advice `--> `casework_nsip_advice_dim`

`casework_nsip_advice_dim` --> `nsip_s51_advice`

```