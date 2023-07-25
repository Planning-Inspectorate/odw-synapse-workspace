# Standardised Introduction
The standardised layer makes the raw layer, which is a sink of data from various PINS systems, available in Delta tables. These tables are then able to be queried and transformed into a data model in harmonised. The function of Raw to Standardised, then, is simply to take csv and excel files which arrive into the ODW and put them into tables that can be queried and transformed with Spark SQL. We do so by defining the raw sources in our orchestration.json which is a key part of this layer.

## Understanding Orchestration.json

- Head over to `infrastructure/configuration/data-lake/orchestration/orchestration.json`. This file contains an array of definitions where each definition is a raw source. Consider the following definition for an example

```
{
	"Source_ID": 1,
	"Source_Folder": "Fileshare/SAP_HR/HR",
	"Source_Frequency_Folder": "Weekly",
	"Source_Folder_Date_Format": "YYYY-MM-DD",
	"Source_Filename_Format": "Addresses - YYYYMMDD.XLSX",
	"Source_Filename_Start": "Addresses - ",
	"Source_Filename_Date_Format": "YYYYMMDD",
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
-   **Source_Folder_Date_Format**: This is the format of the last folder in the folder structure within which our file sits. For the above example, this might be [synapse_data_lake]/odw-raw/Fileshare/SAP_HR/HR/Weekly/2022-10-01/
-   **Source_Filename_Format**: This entry describes the format of the name of the source file
-   **Source_Filename_Start**: This string should be contained within our filename and uniquely identify a single file within our example folder structure above. So, for ‘Addresses - 20221001.XLSX’, ‘Addresses - ‘ would be able to uniquely identify the file. Only one file with this naming should be available in the dated folder.
-   **Source_Filename_Date_Format**: The format of the date in the filename. ‘None’ if none in use.
-   **Completion_Frequency_CRON**: This CRON notation determines the frequency at which we would expect a file for this Source_ID to arrive. Use [crontab guru](https://crontab.guru/) to translate into plaintext what each CRON means.
-   **Expected_Within_Weekdays**: The Completion_Frequency_CRON parameter can determine when the last expected date for the file was, but to determine when we expect the file by, we add the number of days on according to this parameter.
-   **Standardised_Path**: This is the path to the standardised Delta table storage folder in the synapse data lake storage within [synapse_data_lake]/odw-standardised/
-   **Standardised_Table_Name**: This is the name of the standardised Delta table
-   **Standardised_Table_Definition**: This is the location of the Json schema for the standardised Delta table, and will be located within [synapse_data_lake]/odw-config/standardised_table_definitions

## Adding a new Raw Source

In order to add a new raw source and convert the data in standardised form, follow the following steps

- Create a new branch and add a new record in the `orchestration.json` file. This new record will have a new `Source_ID` which we will use at a later step.
- Generate a PR of your branch and after it gets merged, head over to the [Azure DevOps](https://dev.azure.com/planninginspectorate/operational-data-warehouse/_build) and run the **Platform Deploy** pipeline. This will deploy your changes into Synapse.
- When the pipeline succeeds, go to `[synapse_data_lake]/odw-config/orchestration/orchestration.json` and verify that your change has been deployed.
- Go to the pipeline `pln_raw_to_standardised_e2e` and set the new `Source_ID` and the source date folder as the parameters of the pipeline.
	- Click the activity Set SourceID > Settings > Edit the Value of the variable
- Run the pipeline and cross your fingers. If it succeeds, you should be able to find your Standardised table at the specified location with the data ingested.
- Finally, go to `[synapse_data_lake]/odw-config/standardised_table_definitions/` and download the newly created schema json file and push it in your branch. Delete this json file then from synapse so that terraform can deploy it without "file exists" error. This will help keep things consistent between environments.


## Pipeline

## Workspace

## Integrations

## Monitoring & Alerting