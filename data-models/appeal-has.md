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

        class Horizon_ODW_vw_CaseSpecialisms {
            caseRef: str
        }

        class Horizon_ODW_vw_CaseDates {
            casenodeId: str
        }

        class Horizon_ODW_vw_CaseDocumentDatesDates {
            casenodeId: str
        }

        class Horizon_ODW_vw_CaseSiteStrings {
            casenodeId: str
        }

        class Horizon_TypeOfProcedure {
            Id: str
        }

        class Horizon_vw_BIS_AddAdditionalData {
            appealrefNo: str
        }

        class Horizon_vw_BIS_AdditionalFields {
            caseRef: str
        }

        class Horizon_ODW_vw_AdvertAttributes {
            advertAttrKey: str
        }

        class Horizon_vw_curr_TypeOfLevel {
            id: int
        }

        class Horizon_vw_BIS_SpecialistCaseDates {
            appealrefNo: str
        }

        class Horizon_vw_BIS_PlanningAppStrings {
            casenodeId: str
        }

        class Horizon_vw_BIS_PlanningAppDates {
            casenodeId: str
        }

        class Horizon_vw_BIS_LeadCase {
            casenodeId: str
        }

        class Horizon_vw_BIS_CaseStrings {
            casenodeId: str
        }

        class Horizon_vw_BIS_CaseInfo {
            appealrefNo: str
        }

        class Horizon_vw_BIS_CaseDates {
            casenodeId: str
        }

        class Horizon_vw_BIS_AppealsAdditonalData {
            appealrefNo: str
        }

        class Horizon_vw_BIS_CaseSiteCategoryAdditionalStr {
            appealrefNo: str
        }

        class VW_BIS_Inspector_Cases {
            appealrefNo: str
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

        class appeals_has_final {
            caseId: long
        }

    }

`ServiceBus_pins-sb-appeals-bo-dev-appeal-has` --> `sb_appeal_has`
`Horizon_ODW_vw_CaseSpecialisms` --> `horizon_appeal_has`
`Horizon_ODW_vw_CaseDates` --> `horizon_appeal_has`
`Horizon_ODW_vw_CaseDocumentDatesDates` --> `horizon_appeal_has`
`Horizon_ODW_vw_CaseSiteStrings` --> `horizon_appeal_has`
`Horizon_TypeOfProcedure` --> `horizon_appeal_has`
`Horizon_vw_BIS_AddAdditionalData` --> `horizon_appeal_has`
`Horizon_vw_BIS_AdditionalFields` --> `horizon_appeal_has`
`Horizon_ODW_vw_AdvertAttributes` --> `horizon_appeal_has`
`Horizon_vw_curr_TypeOfLevel` --> `horizon_appeal_has`
`Horizon_vw_BIS_SpecialistCaseDates` --> `horizon_appeal_has`
`Horizon_vw_BIS_PlanningAppStrings` --> `horizon_appeal_has`
`Horizon_vw_BIS_PlanningAppDates` --> `horizon_appeal_has`
`Horizon_vw_BIS_LeadCase` --> `horizon_appeal_has`
`Horizon_vw_BIS_CaseStrings` --> `horizon_appeal_has`
`Horizon_vw_BIS_CaseInfo` --> `horizon_appeal_has`
`Horizon_vw_BIS_CaseDates` --> `horizon_appeal_has`
`Horizon_vw_BIS_AppealsAdditonalData` --> `horizon_appeal_has`
`Horizon_vw_BIS_CaseSiteCategoryAdditionalStr` --> `horizon_appeal_has`
`VW_BIS_Inspector_Cases` --> `horizon_appeal_has`
`sb_appeal_has` --> `appeal_has_stg`
`horizon_appeal_has` --> `appeal_has_stg`
`appeal_has_stg` --> `appeal_has_hrm`
`appeal_has_hrm` --> `appeals_has_final`


```
