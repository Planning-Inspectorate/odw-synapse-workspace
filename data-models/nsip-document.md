#### ODW Data Model

##### entity: nsip-document

Data model for nsip-document entity showing data flow from source to curated.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class nsip_document_sb_src {
            documentId: int
        }

        class Horizon_ODW_vw_DocumentMetadata_src {
            dataId: int
        }

        class AIE_Extracts_std_src {
            documentId: int
        }
    }
    
    namespace Standardised {

        class sb_nsip_document {
            documentId: int
        }

        class document_meta_data {
            dataId: int
        }

        class aie_document_data {
            documentId: int
        }
    }

    namespace Harmonised {

        class aie_document_data_hrm {
            documentId: int
        }

        class nsip_document {
            documentId: int
        }
    }

    namespace Curated {

        class nsip_document_curated {
            documentId: int
        }
    }

`nsip_document_sb_src` --> `sb_nsip_document`
`Horizon_ODW_vw_DocumentMetadata_src` --> `document_meta_data`
`AIE_Extracts_std_src` --> `aie_document_data`

`sb_nsip_document` --> `nsip_document`
`document_meta_data` --> `nsip_document`
`aie_document_data` --> `aie_document_data_hrm`
`aie_document_data_hrm` --> `nsip_document`

`nsip_document` --> `nsip_document_curated`


```

Tables and views
- Standardised
  - odw_standardised_db.sb_nsip_document
  - odw_standardised_db.document_meta_data
  - odw_standardised_db.aie_document_data
- Harmonised
  - odw_harmonised_db.sb_nsip_document (service bus staging in harmonised)
  - odw_harmonised_db.nsip_document (final harmonised for NSIP documents)
  - odw_harmonised_db.aie_document_data (harmonised AIE document data)
- Curated
  - odw_curated_db.nsip_document (external curated table)


Orchestration and lineage
- Pipelines
  - workspace/pipeline/pln_service_bus_nsip_document.json (ingest service bus to std sb_nsip_document)
  - workspace/pipeline/pln_horizon_document_metadata.json (standardise Horizon document metadata)
  - workspace/pipeline/pln_aie_document_data.json (standardise and harmonise AIE document data)
  - workspace/pipeline/pln_document_metadata_main.json (orchestrates NSIP doc flow incl. service bus, Horizon, AIE)
  - workspace/pipeline/pln_curated.json (writes curated nsip_document)
- Notebooks and SQL scripts
  - workspace/notebook/py_sb_horizon_harmonised_nsip_document.json (builds odw_harmonised_db.nsip_document from std inputs)
  - workspace/notebook/nsip_document.json (builds odw_curated_db.nsip_document from harmonised)
  - workspace/notebook/py_horizon_harmonised_aie_document.json (harmonises AIE document data)
  - workspace/sqlscript/document_meta_data.json (creates ODW.vw_document_meta_data)
```
