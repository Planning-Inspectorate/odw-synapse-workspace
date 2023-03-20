# Overview
The operational data warehouse (ODW) is made up of 5 logical layers, each of work serves a specific purpose and operates as an entirely independant function of the platform.
To understand how to contribute to, improve and manage each layer of the platform, a set of guides has been provided within this documentation folder.

## 1. Raw
The function of 'raw' is to integrate with source data systems, collect source data and store source ready for onward use.
[Raw](1_raw.md)

## 2. Standardised
The function of 'standardised' is to conform data from raw to a standard parquet format that can be used to model data from this point onwards.
[Standardised](2_standardised.md)

## 3. Harmonised
The function of 'harmonised' is to conform data to an approved enterprise semantic data model and to address any issues with data quality. From this point onwards the data should be
- trusted
- easy to understand
- easy to use
[Harmonised](3_harmonised.md)

## 4. Curated
The function of
[HCurated](4_curated.md)

## 5. Publish / Service Bus
The function of
[Publish](5_publish.md)
