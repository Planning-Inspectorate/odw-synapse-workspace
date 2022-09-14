# PINS Synapse Workload
This module deploys a Synapse Workspace and supporting infrastructure comprising The Planning Inspectorate's (PINS) Operational Data Warehouse (ODW).

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage
All ODW infrastructure components are described using Terraform and deployed using an Azure DevOps Pipeline. This root Terraform module calls upon several child modules to deploy the various components that comprise the ODW architecture in-line with Microsoft Cloud Adoption Framework (CAF) best-practice.

### Contributing
To contribute to this Terraform codebase, it is recommended to install several tools to aid development. All packages may be installed with the Brew package manager on Mac or Linux. Windows users are encouraged to install Windows Subsystem for Linux (WSL) with an Ubuntu image.

#### Install TFEnv and Terraform
TFEnv is an optional tool enabling the installation of multiple versions of Terraform. It is useful in case the developer is working on seveal Terraform projects pinned to specific Terraform versions:
```bash
brew install tfenv
tfenv install 1.2.6
```

#### Install Pre-Commit Hooks
This project supports the use of Pre-Commit. Pre-Commit is a tool that can run checks against your code upon committing changes to a branch. Checks need to pass, otherwise the changes will not be committed. Some checks will automatically fix files (e.g. trailing whitespace). In particular, this tool supports Terraform-specific checks to ensure Terraform code is formatted correctly and executable:
```bash
brew install pre-commit
pre-commit install
```

#### Install Terraform-Docs
Terraform-Docs scans Terraform code and automatically generates documentation for any modules. Documentation includes input variables, outputs, dependencies and more. Documentation is saved as a `README` file in the root of each module. The Pre-Commit hook for Terraform-Docs will ensure documtnation is kept up to date as the Terraform codebase is updated:
```bash
brew install terraform-docs
```

#### Install TFLint
TFLint is a linting tool for Terraform that ensures the Terraform codebase is correct and executable. Warnings will be provided for unsupported values, redudnant variables, and many more checks. The TFLint Pre-Commit hook will run these checks automatically upon commit. After installation, TFLint must be initialised from the root of the repo, and makes use of the `.tflint.hcl` file for its configuration:
```bash
brew install tflint
tflint --init
```

#### Install Checkov
Checkov is a security tool for Terraform that ensures resources defined for a specific provider (in this case Azure) meet security best practice. For example, Checkov will warn if storage accounts are not configured to use TLS1.2. The Pre-Commit hook will run these checks automatically upon commit:
```bash
brew install checkov
```

In some scenarios Checkov may report configuration issues which are intentional or not of concern in practice. In order to bypass checks, a comment may be added to the Terraform resource it warns about. The comment should start with `checkov:skip=` followed by the failed check ID (e.g. `CKV_AZURE_109`) and finally a comment providing context on why the check should be ignored:
```
resource "azurerm_storage_account" "my_storage_account" {
  #checkov:skip=CKV_AZURE_109: Skip reason

  ...
}
```

### Infrastructure Directory Structure
The repository structure is as follows:
```
.
+-- environments
|   +-- {env}.tfbackend
|   +-- {env}.tfvars
|
+-- modules
|   +-- bastion-host
|   +-- synapse-ingestion
|   +-- synapse-management
|   +-- synapse-monitoring
|   +-- synapse-network
|   +-- synapse-workspace-private
|
+-- locals.tf
+-- provider.tf
+-- README.md
+-- region.tf
+-- resource-groups.tf
+-- variables.tf
+-- workload.tf
```

The `environments` folder contains two types of file for each environment:

- `{env}.tfbackend` contains the name of the state file for each environment. This is used alongside the `backend` configuration within the `provider.tf` file and ensures each environment deployment maintaines an independent Terraform state.
- `{env}.tfvars` acts as the main configuration file for the entire workload. The variables from each child module are abstracted to this file, therefore allowing each environment to be defined independently.

The `modules` folder contains the child modules used by this workload:

- `bastion-host`
- `synapse-ingestion`
- `synapse-management`
- `synapse-monitoring`
- `synapse-network`
- `synapse-workspace-private`

All other loose files within the `infrastructure` comprise the root Terraform module, and define the PINS ODW workload specifics:

- `locals.tf` defines static attributes passed into each module such as the service name, Azure resource tags, and a resource suffix applied to all resources made up of the service name, region, and environment.
- `provider.tf` defines both the Terraform and AzureRM provider versions in-use within this workload. The backend state file storage details are also specified here in conjunction with the environment-specific `{env}.tfbackend` files in the `environments` directory.
- `README.md` this file, describing how to contribute to the PINS ODW infrastructure codebase, and the requirements, providers, inputs, and outputs of this root Terraform module.
- `region.tf` a third-party module used to easily reference an Azure region in one of the many available formats. For example one of "UK South", "uks", "uk-south", or "uk_south" may be required for specific scenarios.
- `resource-groups.tf` defines the resource groups as used in this workload, in-line with the Cloud Adoption Framework (CAF) recommendations for a data landing zone.
- `variables.tf` a list of all variables abstracted from the child modules. The values for many of these variables are defined in the environment-specific `{env}.tfvars` files.
- `workload.tf` the main file used to deploy the solution. Calls upon each of the child modules in a specific order and provides values for the input variables.

### Environments
The PINS ODW infrastructure is deployed to three environments which manifest themselves in the form of seperate Azure subscriptions:

| Subscription Name | Subscription ID |
|-------------------|-----------------|
| pins-odw-data-dev-sub | ff442a29-fc06-4a13-8e3e-65fd5da513b3 |
| pins-odw-data-preprod-sub | 6b18ba9d-2399-48b5-a834-e0f267be122d |
| pins-odw-data-prod-sub | a82fd28d-5989-4e06-a0bb-1a5d859f9e0c |

#### Environment-Specific Deployments
Upon deployment, the resource groups defined in `resource-groups.tf` are deployed to the target environment. The child modules are then called upon in `workload.tf` to orchestrate the deployment of the ODW infrastructure components.

The deployment pipeline ultimately runs the `terraform apply` command to provision the ODW infrastructure to the target environment. The specific environment is targeted at two key points:

- `terraform init` initialises the Terraform code in the `infrastructure` directory and selects the environment-specific backend state file with the `-backend-config` argument. The value for the backend config file will vary depending on the selected environment name (`dev.tfbackend`, `test.tfbackend`, `prod.tfbackend`) as selected in the deployment pipeline (`dev`, `test`, `prod`). The `tfbackend` files are located in the `environments` sub-directory.
- `terraform plan` runs the initilised Terraform codebase in the `infrastructure` directory using the environment-specific input variable values with the `-var-file` argument. The value for the input variables will vary depending on the selected environment name (`dev.tfvars`, `test.tfvars`, `prod.tfvars`) as selected in the deployment pipeline (`dev`, `test`, `prod`). The `tfvars` files are located in the `environments` sub-directory. The Terraform plan produced with this command is then passed in to the `terraform apply` command to apply the necessary changes to the ODW infrastructure.

### Pipelines
There are two main infrastructure pipelines:

- `Terraform CI` runs checks against the Terraform code, similar to the Pre-Commit hooks that run when developing locally if using Pre-Commit as recommended in the Contributing section. The CI pipeline should pass before considering a deployment. Any issues brought up by the CI pipeline should be rectified as it could cause issues with deployments otherwise.
- `Terraform CD` ultimately runs `terraform plan` and `terraform apply` against the Terraform codebase in the repos `infrastructure` directory. By default, the main branch is referenced and the `dev` environment set as the target for deployment. Both the branch to deploy from and the target environment can be changed as required.

#### Terraform CI
The below table outlines each step of the `Terraform CI` pipeline:

| Step | Description |
|------|-------------|
| Checkout Repo | Checks-out the Planning Inspectorate ODW-Service repo |
| Setup | Downloads the required tools for the CI pipeline (TFLint, Checkov) |
| Azure Login | Authenticates to Azure to verify credentials and access |
| Terrform Init | Initialises the Terraform codebase and backend state file |
| Terraform Format | Checks the format of Terraform files |
| Terraform Validate | Validates Terraform files to ensure they are executable |
| TFLint Validate | Runs the TFLint tool for linting and validation purposes |
| Checkov Validate | Runs the Checkov tool to detect security misconfigurations |

#### Terraform CD
The below tables outline the steps in each stage of the `Terraform CD` pipeline:

| Step | Description |
|------|-------------|
| Checkout Repo | Checks-out the Planning Inspectorate ODW-Service repo |
| Azure Login | Authenticates to Azure to verify credentials and access |
| Terraform Plan | Initialises and runs `terraform plan` using environment-specific files |
| Create Artifact | Uploads the Terraform plan file as a pipeline artifact |

| Step | Description |
|------|-------------|
| Checkout Repo | Checks-out the Planning Inspectorate ODW-Service repo |
| Download Artifact | Downloads the Terraform plan file |
| Verify Artifact | Confirms the plan file exists on the DevOps agent as expected |
| Azure Login | Authenticates to Azure to verify credentials and access |
| Terraform Apply | Initialises and runs `terraform apply` using the downloaded Terraform plan file |

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.1.6, < 2.0.0 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | ~> 3.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.22.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |
| <a name="module_bastion_host"></a> [bastion\_host](#module\_bastion\_host) | ./modules/bastion-host | n/a |
| <a name="module_synapse_data_lake"></a> [synapse\_data\_lake](#module\_synapse\_data\_lake) | ./modules/synapse-data-lake | n/a |
| <a name="module_synapse_ingestion"></a> [synapse\_ingestion](#module\_synapse\_ingestion) | ./modules/synapse-ingestion | n/a |
| <a name="module_synapse_management"></a> [synapse\_management](#module\_synapse\_management) | ./modules/synapse-management | n/a |
| <a name="module_synapse_monitoring"></a> [synapse\_monitoring](#module\_synapse\_monitoring) | ./modules/synapse-monitoring | n/a |
| <a name="module_synapse_network"></a> [synapse\_network](#module\_synapse\_network) | ./modules/synapse-network | n/a |
| <a name="module_synapse_sql_server"></a> [synapse\_sql\_server](#module\_synapse\_sql\_server) | ./modules/synapse-sql-server | n/a |
| <a name="module_synapse_workspace_private"></a> [synapse\_workspace\_private](#module\_synapse\_workspace\_private) | ./modules/synapse-workspace-private | n/a |

## Resources

| Name | Type |
|------|------|
| [azurerm_resource_group.data](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.data_management](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.ingestion](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.monitoring](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.network](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.sql_server](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_alert_group_platform_enabled"></a> [alert\_group\_platform\_enabled](#input\_alert\_group\_platform\_enabled) | Determines whether the alert group for platform alerts is enabled | `bool` | `false` | no |
| <a name="input_alert_group_platform_recipients"></a> [alert\_group\_platform\_recipients](#input\_alert\_group\_platform\_recipients) | A list of email recipients to recieve platform alerts | `list(string)` | `[]` | no |
| <a name="input_alert_group_synapse_enabled"></a> [alert\_group\_synapse\_enabled](#input\_alert\_group\_synapse\_enabled) | Determines whether the alert group for Synapse alerts is enabled | `bool` | `false` | no |
| <a name="input_alert_group_synapse_recipients"></a> [alert\_group\_synapse\_recipients](#input\_alert\_group\_synapse\_recipients) | A list of email recipients to recieve Synapse alerts | `list(string)` | `[]` | no |
| <a name="input_alert_threshold_data_lake_capacity_bytes"></a> [alert\_threshold\_data\_lake\_capacity\_bytes](#input\_alert\_threshold\_data\_lake\_capacity\_bytes) | The threshold at which to trigger an alert for exceeding Data Lake capacity in bytes | `number` | `1099511627776` | no |
| <a name="input_bastion_host_enabled"></a> [bastion\_host\_enabled](#input\_bastion\_host\_enabled) | Determines if a Bastion Host should be provisioned for management purposes | `bool` | `false` | no |
| <a name="input_bastion_vm_image"></a> [bastion\_vm\_image](#input\_bastion\_vm\_image) | An object describing the image specification to use for the Bastion jumpbox VM | `map(string)` | <pre>{<br>  "offer": "windows-11",<br>  "publisher": "MicrosoftWindowsDesktop",<br>  "sku": "win11-21h2-ent",<br>  "version": "latest"<br>}</pre> | no |
| <a name="input_bastion_vm_size"></a> [bastion\_vm\_size](#input\_bastion\_vm\_size) | The size of the Bastion jumpbox VM to be deployed | `string` | `"Standard_F2s_v2"` | no |
| <a name="input_bastion_vm_username"></a> [bastion\_vm\_username](#input\_bastion\_vm\_username) | The Windows administrator username for the Bastion jumpbox VM | `string` | `"basadmin"` | no |
| <a name="input_data_lake_account_tier"></a> [data\_lake\_account\_tier](#input\_data\_lake\_account\_tier) | The tier of the Synapse data lake Storage Account | `string` | `"Standard"` | no |
| <a name="input_data_lake_replication_type"></a> [data\_lake\_replication\_type](#input\_data\_lake\_replication\_type) | The replication type for the Synapse data lake Storage Account | `string` | `"ZRS"` | no |
| <a name="input_data_lake_retention_days"></a> [data\_lake\_retention\_days](#input\_data\_lake\_retention\_days) | The number of days blob and queue data will be retained for upon deletion | `number` | `7` | no |
| <a name="input_data_lake_role_assignments"></a> [data\_lake\_role\_assignments](#input\_data\_lake\_role\_assignments) | An object mapping RBAC roles to principal IDs for the data lake Storage Account | `map(list(string))` | `{}` | no |
| <a name="input_data_lake_storage_containers"></a> [data\_lake\_storage\_containers](#input\_data\_lake\_storage\_containers) | A list of container names to be created in the Synapse data lake Storage Account | `list(string)` | <pre>[<br>  "default"<br>]</pre> | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_role_assignments"></a> [key\_vault\_role\_assignments](#input\_key\_vault\_role\_assignments) | An object mapping RBAC roles to principal IDs for Key Vault | `map(list(string))` | `{}` | no |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_network_watcher_enabled"></a> [network\_watcher\_enabled](#input\_network\_watcher\_enabled) | Determines whether a Network Watcher resource will be deployed | `bool` | `false` | no |
| <a name="input_spark_pool_enabled"></a> [spark\_pool\_enabled](#input\_spark\_pool\_enabled) | Determines whether a Synapse-linked Spark pool should be deployed | `bool` | `false` | no |
| <a name="input_spark_pool_max_node_count"></a> [spark\_pool\_max\_node\_count](#input\_spark\_pool\_max\_node\_count) | The maximum number of nodes the Synapse-linked Spark pool can autoscale to | `number` | `9` | no |
| <a name="input_spark_pool_min_node_count"></a> [spark\_pool\_min\_node\_count](#input\_spark\_pool\_min\_node\_count) | The minimum number of nodes the Synapse-linked Spark pool can autoscale to | `number` | `3` | no |
| <a name="input_spark_pool_node_size"></a> [spark\_pool\_node\_size](#input\_spark\_pool\_node\_size) | The size of nodes comprising the Synapse-linked Spark pool | `string` | `"Small"` | no |
| <a name="input_spark_pool_version"></a> [spark\_pool\_version](#input\_spark\_pool\_version) | The version of Spark running on the Synapse-linked Spark pool | `string` | `"2.4"` | no |
| <a name="input_sql_pool_collation"></a> [sql\_pool\_collation](#input\_sql\_pool\_collation) | The collation of the Synapse-linked dedicated SQL pool | `string` | `"SQL_Latin1_General_CP1_CI_AS"` | no |
| <a name="input_sql_pool_enabled"></a> [sql\_pool\_enabled](#input\_sql\_pool\_enabled) | Determines whether a Synapse-linked dedicated SQL pool should be deployed | `bool` | `false` | no |
| <a name="input_sql_pool_sku_name"></a> [sql\_pool\_sku\_name](#input\_sql\_pool\_sku\_name) | The SKU of the Synapse-linked dedicated SQL pool | `string` | `"DW100c"` | no |
| <a name="input_sql_server_administrator_username"></a> [sql\_server\_administrator\_username](#input\_sql\_server\_administrator\_username) | The SQL administrator username for the SQL Server | `string` | `"sqladmin"` | no |
| <a name="input_sql_server_enabled"></a> [sql\_server\_enabled](#input\_sql\_server\_enabled) | Determins whether a SQL Server should be deployed | `string` | `false` | no |
| <a name="input_synapse_aad_administrator"></a> [synapse\_aad\_administrator](#input\_synapse\_aad\_administrator) | A map describing the username and Azure AD object ID for the Syanapse administrator account | `map(string)` | n/a | yes |
| <a name="input_synapse_data_exfiltration_enabled"></a> [synapse\_data\_exfiltration\_enabled](#input\_synapse\_data\_exfiltration\_enabled) | Determines whether the Synapse Workspace should have data exfiltration protection enabled | `bool` | `false` | no |
| <a name="input_synapse_role_assignments"></a> [synapse\_role\_assignments](#input\_synapse\_role\_assignments) | An object mapping RBAC roles to principal IDs for the Synapse Workspace | `map(list(string))` | `{}` | no |
| <a name="input_synapse_sql_administrator_username"></a> [synapse\_sql\_administrator\_username](#input\_synapse\_sql\_administrator\_username) | The SQL administrator username for the Synapse Workspace | `string` | `"synadmin"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |
| <a name="input_vnet_base_cidr_block"></a> [vnet\_base\_cidr\_block](#input\_vnet\_base\_cidr\_block) | The base IPv4 range for the Virtual Network in CIDR notation | `string` | `"10.90.0.0/24"` | no |
| <a name="input_vnet_subnets"></a> [vnet\_subnets](#input\_vnet\_subnets) | A collection of subnet definitions used to logically partition the Virtual Network | `list(map(string))` | <pre>[<br>  {<br>    "name": "ManagementSubnet",<br>    "new_bits": 2<br>  },<br>  {<br>    "name": "SynapseEndpointSubnet",<br>    "new_bits": 2<br>  },<br>  {<br>    "name": null,<br>    "new_bits": 2<br>  },<br>  {<br>    "name": null,<br>    "new_bits": 2<br>  }<br>]</pre> | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_data_lake_account_id"></a> [data\_lake\_account\_id](#output\_data\_lake\_account\_id) | The ID of the Data Lake Storage Account |
| <a name="output_data_lake_dfs_endpoint"></a> [data\_lake\_dfs\_endpoint](#output\_data\_lake\_dfs\_endpoint) | The DFS endpoint URL of the Data Lake Storage Account |
| <a name="output_data_resource_group_name"></a> [data\_resource\_group\_name](#output\_data\_resource\_group\_name) | The name of the data application resource group |
| <a name="output_key_vault_uri"></a> [key\_vault\_uri](#output\_key\_vault\_uri) | The URI of the Key Vault |
| <a name="output_service_bus_namespace_name"></a> [service\_bus\_namespace\_name](#output\_service\_bus\_namespace\_name) | The name of the Service Bus Namespace |
| <a name="output_synapse_dev_endpoint"></a> [synapse\_dev\_endpoint](#output\_synapse\_dev\_endpoint) | The development connectivity endpoint for the Synapse Workspace |
| <a name="output_synapse_dsql_endpoint"></a> [synapse\_dsql\_endpoint](#output\_synapse\_dsql\_endpoint) | The dedicated SQL pool connectivity endpoint for the Synapse Workspace |
| <a name="output_synapse_ssql_endpoint"></a> [synapse\_ssql\_endpoint](#output\_synapse\_ssql\_endpoint) | The serverless SQL pool connectivity endpoint for the Synapse Workspace |
| <a name="output_synapse_workspace_name"></a> [synapse\_workspace\_name](#output\_synapse\_workspace\_name) | The name of the Synapse Workspace |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
