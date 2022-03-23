# Introduction 
This repo contains all the artifacts for the PINS Operational Data Warehouse Pilot/POC. It consists of the following:-
* [Azure Synapse Workspace](workspace/) - Synapse artifacts e.g. pipelines, datasets, notebooks, linked services
* [Azure DevOps](devops/) - Azure DevOps pipeline configuration for build/release pipelines
* [Azure Service Bus](servicebus/) - js code for publishing/consuming service bus topics
* [Terraform](terraform/) - terraform templates used for generating ODW infrastructure as code
* [Test Scripts](tests/) - scripts for generating synthetic data for testing purposes

# Reference Documentation
* Azure Data Landscape
  * [Azure data management and analytics scenario](https://docs.microsoft.com/en-us/azure/cloud-adoption-framework/scenarios/data-management/)
  * [Analytics end-to-end with Azure Synapse](https://docs.microsoft.com/en-us/azure/architecture/example-scenario/dataplate2e/data-platform-end-to-end?tabs=portal)
  * [Synapse workspaces and lakehouses best practice](https://techcommunity.microsoft.com/t5/azure-synapse-analytics-blog/the-best-practices-for-organizing-synapse-workspaces-and/ba-p/3002506)
* Azure Synapse
  * [Source Control](https://docs.microsoft.com/en-us/azure/synapse-analytics/cicd/source-control)
  * [CI/CD](https://docs.microsoft.com/en-us/azure/synapse-analytics/cicd/continuous-integration-delivery)
  * [Access Control](https://docs.microsoft.com/en-us/azure/synapse-analytics/security/how-to-set-up-access-control) - these rules are implemented in the Terraform templates
* Terraform
  * [CI/CD Terraform Deployment with Azure DevOps](https://www.azuredevopslabs.com/labs/vstsextend/terraform/)


# Getting Started
The following steps outline how to get up and running with this repo on your own system:
1.  Environment access
    1.  Github access - if you're reading this repo readme you probably already have this
    2.  Azure Portal access - additional access is required to the Azure Portal and the corresponding [Azure Resources in each environment](#environments)
2.  Application Installation - the following desktop applications are optional but provide advantages when working with some of the Azure resources - PINS Azure auth policy is to restrict access to PINS devices only so non-PINS devices will need to be whitelisted to use these
      1. Install [Visual Studio Code](https://code.visualstudio.com/) or equivalent IDE - for editing and commiting code artifacts
      2. Install [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio) - for connecting to Azure SQL instances and managing/commiting data notebooks
      3. Install [Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
3.  Clone Repo
    1. Create a Personal Access Token in GitHub or use another authentication method e.g. SSH
    2. Clone the repo in VSCode/Azure Data Studio to a local folder

# Environments
|       Resources      | Description | Dev                               | Location |      Preprod                 | Location | Production | Location |
|:--------------------:|:-----------:|:---------------------------------:|:--------:|:----------------------------:|:--------:|:----------:|:--------:|
| Resource Group       | Azure Container holding related environment resources | [pins-odw-data-dev-rg](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-data-dev-rg/)       | UK South  | [pins-odw-data-preprod-rg](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/6b18ba9d-2399-48b5-a834-e0f267be122d/resourceGroups/pins-odw-data-preprod-rg)       | UK South  | |  |
| Key Vault       | Secure store for environment secrets and credentials | [pins-odw-data-dev-kv](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-data-dev-rg/providers/Microsoft.KeyVault/vaults/pins-odw-data-dev-kv/overview)       | UK South  | [pins-odw-data-preprod-kv](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/6b18ba9d-2399-48b5-a834-e0f267be122d/resourceGroups/pins-odw-data-preprod-rg/providers/Microsoft.KeyVault/vaults/pins-odw-data-preprod-kv/overview)       | UK South  |  |  |
| Azure Synapse Analytics  | Azure platform for data orchestration, notebooks, workspaces and SQL Pools | [pins-odw-data-dev-syn-ws](https://web.azuresynapse.net/en/home?workspace=%2Fsubscriptions%2Fff442a29-fc06-4a13-8e3e-65fd5da513b3%2FresourceGroups%2Fpins-odw-data-dev-rg%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2Fpins-odw-data-dev-syn-ws)                      | UK South  | [pins-odw-data-preprod-syn-ws](https://web.azuresynapse.net/en/home?workspace=%2Fsubscriptions%2F6b18ba9d-2399-48b5-a834-e0f267be122d%2FresourceGroups%2Fpins-odw-data-preprod-rg%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2Fpins-odw-data-preprod-syn-ws)                  | UK South  |  |  |
| Data Lake Gen2 storage         | Azure storage for data lakehouse | [pinsodwdatadevstorage](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-data-dev-rg/providers/Microsoft.Storage/storageAccounts/pinsodwdatadevstorage/overview)                     | UK South  | [pinsodwdatapreprodstor](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/6b18ba9d-2399-48b5-a834-e0f267be122d/resourceGroups/pins-odw-data-preprod-rg/providers/Microsoft.Storage/storageAccounts/pinsodwdatapreprodstor/overview)                 | UK South  |   |  |
| Temporary Terraform State Storage           | Temporary storage for Terraform state - should be relocated alongside other PINS Terraform state storage | [pinsodwterraformstorage](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-terraform-rg/providers/Microsoft.Storage/storageAccounts/pinsodwterraformstorage/overview)                 | UK South  | N/A |   | N/A |  |
| Temporary Service Bus Resource Group           | Temporary resource group for Service Bus outside of Data Landing Zone | [pins-odw-service-dev-rg](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-service-dev-rg/overview)                 | UK South  | N/A |   | N/A |  |
| Temporary Service Bus          | Temporary Service Bus for POC of ODW integration| [pinsodwservicedev](https://portal.azure.com/#@planninginspectorate.gov.uk/resource/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-odw-service-dev-rg/providers/Microsoft.ServiceBus/namespaces/pinsodwservicedev/overview)                 | UK South  | N/A |  | N/A |  |
