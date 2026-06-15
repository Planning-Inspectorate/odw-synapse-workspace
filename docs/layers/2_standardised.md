# Standardised Introduction
The standardised layer makes the raw layer, which is a sink of data from various PINS systems, available in Delta tables. These tables are then able to be queried and transformed into a data model in harmonised. The function of Raw to Standardised, then, is simply to take csv and excel files which arrive into the ODW and put them into tables that can be queried and transformed with Spark SQL. We do so by defining the raw sources in our orchestration.json which is a key part of this layer.

## Understanding Orchestration.json

- Head over to ODW-config reporo and edit `data-lake/orchestration/orchestration.json`. This file contains an array of definitions where each definition is a raw source. Consider the following definition for an example

```
{
	"Source_ID": 1,
	"Source_Folder": "Fileshare/SAP_HR/HR",
	"Source_Frequency_Folder": "Weekly",
	"Source_Filename_Format": "Addresses - YYYYMMDD.XLSX",
	"Source_Filename_Start": "Addresses - ",
	"Completion_Frequency_CRON": "0 0 * * 1",
	"Expected_Within_Weekdays": 5,
	"Standardised_Path": "HR",
	"Standardised_Table_Name": "hr_addresses",
	"Standardised_Table_Definition": "standardised_table_definitions/addresses.JSON"
}
```

-   **Source_ID** : Unique value for each definition in the array, starting at 1 and increasing in values of 1
-   **Source_Folder**: The folder within [synapse_data_lake]/odw-raw/ that the source file date folders are located. Using the example above, within [synapse_data_lake]/odw-raw/Fileshare/SAP_HR/HR/ we would expect all data pertaining to this Source_ID to sit under this folder.
-   **Source_Frequency_Folder**: If the data is to be received at multiple frequencies, the next folder underneath Source_Folder in the folder structure should be ‘Weekly’ or ‘Monthly’
-   **Source_Filename_Format**: This entry describes the format of the name of the source file
-   **Source_Filename_Start**: This string should be contained within our filename and uniquely identify a single file within our example folder structure above. So, for ‘Addresses - 20221001.XLSX’, ‘Addresses - ‘ would be able to uniquely identify the file. Only one file with this naming should be available in the dated folder.
-   **Expected_Within_Weekdays**: To determine when we expect the file by, we add the number of days on according to this parameter.
-   **Standardised_Path**: This is the path to the standardised Delta table storage folder in the synapse data lake storage within [synapse_data_lake]/odw-standardised/
-   **Standardised_Table_Name**: This is the name of the standardised Delta table
-   **Standardised_Table_Definition**: This is the location of the Json schema for the standardised Delta table, and will be located within [synapse_data_lake]/odw-config/standardised_table_definitions

## Adding a new Raw Source

In order to add a new raw source and convert the data in standardised form, follow the following steps

- Add a new record in the `orchestration.json` file. This new record will have a new `Source_ID` which we will use at a later step.
- Go to `[synapse_data_lake]/odw-config/orchestration/` and replace the existing `orchestration.json` with the updated file.
- Run the pipeline `pln_raw_to_standardised_e2e` and set the `source_id` and the `source_folder_date` parameters. 
  - The `source_id` parameter is the Source_ID of the new record you added in the orchestration.json. 
  - The `source_folder_date` parameter is the name of the dated folder in which your raw file is present. i.e 2024-01-01
- Run the pipeline and cross your fingers. If it succeeds, you should be able to find your Standardised table at the specified location with the data ingested.
- Make sure to add your changes in the `orchestration.json` and `[synapse_data_lake]/odw-config/standardised_table_definitions/{newly_created_schema.json}` to your branch. This will help keep things consistent between environments.


## Anonymisation (DEV / TEST only)

In non-production environments (DEV and TEST) all sensitive columns are anonymised **before** the standardised table is written. Production outputs are never modified.

### How it works

1. The standardisation process checks `Util.is_non_production_environment()`, which reads `spark.executorEnv.environment` from the attached Spark configuration artifact (`DEVConfiguration` / `TESTConfiguration`). Production defaults to `PROD` if the key is absent.
2. When enabled, the `AnonymisationEngine` fetches column classifications from Azure Purview for the asset being processed, then applies strategy-based masking:
   - **Names** → `"REDACTED"`
   - **Email addresses** → SHA-256 hash (preserves join-ability; case-insensitive)
   - **NI numbers** → deterministic fake in `AA000000A` format
   - **Dates of birth** → pseudorandom date 1955–2005 (seeded per row)
   - **Ages / salaries** → pseudorandom integer in a plausible range (seeded per row)
   - **Postcodes / addresses** → outward code only / `"REDACTED"`
3. If anonymisation fails the process raises and **does not write** any data (atomicity preserved).

### Configuration

An optional YAML policy file can restrict which Purview classifications trigger anonymisation and configure per-entity seed columns. Deploy it to:

```text
abfss://odw-config@<storage-account>/anonymisation/policy.yaml
```

A reference copy with all supported classifications is at [`docs/anonymisation/mock_policy.yaml`](../anonymisation/mock_policy.yaml). If the file is absent the engine applies anonymisation to **all** classified columns.

### Observability

Every run emits a structured log line at the anonymisation gate (visible in Application Insights / Synapse Monitor):

```text
anonymisation_gate: environment=DEV enabled=True entity=service-user
```

In PROD the line reads `enabled=False` and no anonymisation is applied. The engine additionally logs the list of columns anonymised and row counts (event `apply_from_purview.summary`).

### Deployment checklist

- [ ] `DEVConfiguration` Spark configuration is attached to the DEV Spark pool.
- [ ] `TESTConfiguration` Spark configuration is attached to the TEST Spark pool.
- [ ] `policy.yaml` is uploaded to `odw-config/anonymisation/` in the target storage account (optional — defaults apply without it).
- [ ] The Synapse workspace service principal has Purview Data Reader role.
- [ ] Verify DEV output: confirm PII columns in `odw_standardised_db` contain masked values, non-PII columns are unchanged.

### Rollback

To disable anonymisation in DEV/TEST without a code change, remove or rename the `spark.executorEnv.environment` key in the attached Spark configuration artifact. The engine will then default to `PROD` behaviour and skip anonymisation.

See [`odw/core/anonymisation/README.md`](../../odw/core/anonymisation/README.md) for full engine documentation and [`docs/anonymisation/INTEGRATION_TESTING_GUIDE.md`](../anonymisation/INTEGRATION_TESTING_GUIDE.md) for testing instructions.

## Pipeline

## Workspace

## Integrations

## Monitoring & Alerting
