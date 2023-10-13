# Naming Standards

## Branches

- **[type]/[short-lowercase-desc-with-hyphens]**
- **Examples:**
  - feature/hr-raw-ingestion
  - fix/invalid-schema-hr-leavers
  - docs/naming-standards
  
## Pipelines

We have a folder for each schema and a *main* pipeline (with a trigger attached to it). Each schema folder will have a *layers* folder which will contain a subfolder for each layer. There might also be a *utils* folder with the utility pipelines. Each folder and pipeline should use the snake_case naming convention. For example, let's assume the casework schema

- **casework**
  - pln_casework_main
  - **layers**
    - **0-raw**
      - pln_one
      - pln_two
      - pln_three
    - **1-standardised**
    - **2-harmonised**
    - **3-curated**
  - **utils**
    - pln_util_one
    - pln_util_two

## Notebooks

We have a folder for each schema and a 'Utils' one for the notebooks used in different processes. Each schema folder might have a *Legacy* folder which will contain the notebooks used for a first data dump on the . There might also be a *utils* folder with the utility pipelines. Each folder and pipeline should use the snake_case naming convention. For example, let's assume the casework schema

- **casework**
  - pln_casework_main
  - **layers**
    - **0-raw**
      - pln_one
      - pln_two
      - pln_three
    - **1-standardised**
    - **2-harmonised**
    - **3-curated**
  - **utils**
    - pln_util_one
    - pln_util_two

