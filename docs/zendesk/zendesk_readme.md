# Code Documentation Zendesk
Document the data journey from source to curated, including
Any transformations from unstructured source to structured format
Any JSON source object nesting
Any business logic used, e.g. to find/identify Case References in Zendesk fields
Link the readme items to examples in the code to provide context
Anything else Zendesk-specific of note
e.g. notes on limitations of unusable data, and on any limitations on how to use code (datatypes hard to ingest into pyspark)

### Data Source:

The data is sourced from ZenDesk in JSON format, which is unstructured. The code processes this data to make it more structured and suitable for analysis.

### Transformation 1: JSON Source Object Nesting

The code reads JSON data from the source files using PySpark and loads it into a DataFrame named shows. The JSON data contains nested objects and arrays, which are automatically parsed and flattened by Spark's JSON reader.

Example: Reading JSON data into a DataFrame - shows = spark.read.json(source_path)

### Transformation 2: Pandas DataFrame for Structuring

The shows DataFrame is converted to a Pandas DataFrame (pandasDF). This step allows for more flexibility in manipulating the data since Pandas DataFrames are easier to work with for certain operations.

Example: Conversion to Pandas DataFrame - pandasDF = shows.toPandas()

### Transformation 3: Data Type Standardization

To ensure uniform data types, the Pandas DataFrame pandasDF is converted to a Spark DataFrame (sparkDF) after applying a transformation to ensure all data is represented as strings.

[Example](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_raw_to_standerdised.json#L155-L156): Standardizing data types -

```python

    pandasDF = pandasDF.applymap(str)

    sparkDF = spark.createDataFrame(pandasDF)

```

[Example](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_harmonised_export.json#L370-L377):  

```python
           "    dfassigneelist=pd.DataFrame(assigneelist)\r\n",
					"    dfassigneelist.id = dfassigneelist.id.astype(str)\r\n",
					"    dfassigneelist.locale_id = dfassigneelist.locale_id.astype(str)\r\n",
					"    dfassigneelist.organization_id = dfassigneelist.organization_id.astype(str)\r\n",
					"    dfassigneelist.role_type = dfassigneelist.role_type.astype(str)\r\n",
					"    dfassigneelist.custom_role_id = dfassigneelist.custom_role_id.astype(str)\r\n",
					"    dfassigneelist.organization_id = dfassigneelist.organization_id.astype(str)\r\n",
					"    dfassigneelist.default_group_id = dfassigneelist.default_group_id.astype(str)
```

### Transformation 4: Adding Constant Values

Several columns are added to sparkDF with constant values. For example, "SourceSystemID" is set to 6, "IngestionDate" is set to the current timestamp, "ValidTo" is set to "NULL," and "IsActive" is set to "Y."

[Example](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_raw_to_standerdised.json#L149-L159): Adding constant values -

```python

    sparkDF = sparkDF.withColumn("SourceSystemID", lit(6))

                                    .withColumn('IngestionDate', F.current_timestamp())

                                    .withColumn('ValidTo', lit("NULL"))

                                    .withColumn('IsActive', lit("Y"))
```

### Transformation 5: Row Numbering

A new column "RowID" is added to the DataFrame. It represents row numbers within partitions defined by "SourceSystemID" and ordered by the "id" column.

Example: Row numbering -

```python

    partition = Window.partitionBy("SourceSystemID").orderBy('id')

    sparkDF = sparkDF.withColumn('RowID', row_number().over(partition))

 ```

### Data Loading:

The final structured data in sparkDF is written to a Delta table named "odw_standardised_db.zendesk_system_extract."

[Example](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_raw_to_standerdised.json#L131): Writing to a Delta table - sparkDF.write.format('delta').saveAsTable("odw_standardised_db.zendesk_system_extract")

### Data Ingestion:

      - Source: JSON files in blob storage.

      - Read and load JSON files using PySpark into a DataFrame called shows.


### Table Creation:

      - Create a Delta table named odw_standardised_db.zendesk_system_extract to store the transformed data.

      - The table is created using the DataFrame sparkDF, which represents the standardized Zendesk data.

Data Processing Functions:

      - The code includes functions for processing various aspects of [Zendesk data](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_harmonised_export.json#L275-560):

          - fields_table(): Extracts and processes fields data.

          - custom_fields_table(): Extracts and processes custom fields data.

          - comments_and_attachments(): Extracts comments and attachments data.

          - assignee_table(): Processes data related to ticket assignees.

          - submitter_table(): Processes data related to ticket submitters.

          - requester_table(): Processes data related to ticket requesters.

          - collaborator_table(): Processes data related to collaborators on tickets.

### Data Validation:

      - The script performs JSON [schema validation](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_schema_validation.json#L147-L167) for Zendesk data using the jsonschema library. It checks if the JSON data conforms to a specified schema. This validation ensures that the data is structured correctly.

### Handling New and Updated Tickets :

      - The code includes functions to handle newly created and updated Zendesk tickets, storing them in separate Delta tables ([odw_standardised_db.zendesk_updated_tickets](https://github.com/Planning-Inspectorate/ODW-Service/blob/main/workspace/notebook/zendesk_get_updated_tickets.json) and [odw_standardised_db.zendesk_new_tickets](https://github.com/Planning-Inspectorate/ODW-Service/blob/main/workspace/notebook/zendesk_get_created_tickets.json).
    -The code includes a [pipeline](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/pipeline/0_Zendesk_API_to_RAW.json) which ingest the data on daily bases through all the layers 

### Merging Data:

      - The script merges historical data from a file with the existing data in the odw_standardised_db.zendesk_system_extract table. This is done using a MERGE INTO SQL statement.

### SQL Queries:

      - The code includes SQL queries to interact with the Delta tables, such as retrieving data and performing counts.

### Logging :

      - There are references to a [@logging_to_appins](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/py_logging_decorator.json) decorator, which include logging to an application insight for monitoring and logging purposes.

### Data journey
 	- Follow belowe link for the data flow schmea 
  https://drive.google.com/file/d/1RpvaOCcGcPAu5-rxpfT0EEqvgh7d80OA/view?usp=sharing

Merging Zendesk with Service user 
The data of the assisgnee, collaborator, requester and sumbitter is unified in one temporary view and then added to the harmonised layer of the serice user table via [odw-harmonised/adding_zendesk_to_serviceuser notebook](https://web.azuresynapse.net/en/authoring/analyze/notebooks/adding_zendesk_to_service_user?workspace=%2Fsubscriptions%2Fff442a29-fc06-4a13-8e3e-65fd5da513b3%2FresourceGroups%2Fpins-rg-data-odw-dev-uks%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2Fpins-synw-odw-dev-uks)

  
### Zendesk steps

Take a system extract from the SAP storgae and save it in the the blob storage (/ZenDesk/Export/export-2023-03-15-1051-10932060-13758561992081e5c3.json)
Since the export file holds multiple zendesk tickets, run the odw-raw/zendesk_standardised_and_harmonised notebook to load the file and separate it in one json per ticket.
Then run zendesk_raw_to standardised to load the export into the standardised table and also create the custom_fileds and fields notebooks which hold the ref number in harmonised layer. Then run the harmonised notebook called zendesk harmonised export to add the remaining nested data fields into tables.
the reason behind creating all different tables from harmonised this way rather than the way other tables were created is because all the data in the zendesk tickets is nested. Also, in addition all tickets are different from each other containing different column names and information. The information in columns is also written by users so it can't always be trusted (eg the custom_fields/fields column has id and value which holds the ticket reference to other table. That info was added by a user manually and the UI does not require the user to put a valid Ref Number)
After the zendesk standardised and harmonised has been build we have created a pipeline called 0_Zendesk_API_to_RAW which pulls every 24 hours the data from the storage using an api and loads it into the standardised table called zendesk_system_extract
In case of any missing data we use the [0_Zendesk_API_to_RAW historical](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/pipeline/0_Zendesk_API_to_RAW_historical_load.json) pipeline whcih downloads all tickets existing in zendesk.
In order to update the standardised table with all the data, go to the [zendesk_historical](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_historical.json) notebook in odw-raw. BEFORE RUNNING THE NOTEBOOK YOU MUST CHNAGE THE NAME OF THE FILE WITH THE NEW ONE. eg go to the zendesk_hist function and change :"with open(f"/synfs/{jobId}/zendesk_items/historical_2023-09-04.json", 'r',encoding="utf-8-sig") as json_file: " to "with open(f"/synfs/{jobId}/zendesk_items/NAME OF THE FILE", 'r',encoding="utf-8-sig") as json_file: ". NAME OF THE FILE can be found [/ZenDesk/historical/historical_2023-09-04.json] and should have the date on which the pipeline 0_Zendesk_API_to_RAW historical was run on.
For validation use notebook [odw-raw/Zendesk-schema_validation](https://github.com/Planning-Inspectorate/ODW-Service/blob/feat/zendesk_documentaition/workspace/notebook/zendesk_schema_validation.json#L50-L100) which holds functions for validating the tickets and can be adjusted for the new once as well. To adjust , chnage the fils which you want to read/open.
Notebooks which need to be run in prod/pre-prod

Odw-raw/Zendesk-standardised-and-harmonised(only once)

Odw-raw/Zendesk-raw-to-standardised

Odw-harmonised/Zendesk-harmonised-export

Daily update

Pipeline zendesk/layer/0-raw 0_zendesk_api_to_raw

Which will run the 2 notebooks Zendesk_get_updated_tickets and Zendesk_get_created_tickets

### Challenges and Findings:

The export.json holds tickets which are different from each other.
The difference is in the schema, as none of them respect a certain schema.
The data is not consistent in all of them.
The linking between existing case can be done using the table called custom_fields/fields which hold an id and a value, value column being the one holding the information of the cases such as the case reference or appeal reference.
The value column in the above tables is also completed by the user, while we(pins) don't put any restrictions on or criteria to what the value filed should be. Hence, we can fine irrelevant data such as "case_reference_choice_no", "(NULL)", "*random name*".
Ideally the column value should hold the following format "APP/X2410/W/21/******", which in some case it has and can be traced back or matched with different appeals or cases.
Data from Zendesk hardly matches the data required in the service user table based on the current schema of the serice user. We can add only bits of information.
