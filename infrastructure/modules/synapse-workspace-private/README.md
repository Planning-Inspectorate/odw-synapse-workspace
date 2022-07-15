# Synapse Workspace Private

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.13.0 |
| <a name="provider_null"></a> [null](#provider\_null) | 3.1.1 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.3.2 |
| <a name="provider_time"></a> [time](#provider\_time) | 0.7.2 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_key_vault_secret.synapse_sql_administrator_password](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_key_vault_secret.synapse_sql_administrator_username](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_private_endpoint.synapse_dedicated_sql_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/private_endpoint) | resource |
| [azurerm_private_endpoint.synapse_development](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/private_endpoint) | resource |
| [azurerm_private_endpoint.synapse_serverless_sql_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/private_endpoint) | resource |
| [azurerm_role_assignment.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_msi_data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_msi_key_vault](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_data_lake_gen2_filesystem.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_data_lake_gen2_filesystem) | resource |
| [azurerm_synapse_firewall_rule.allow_all](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_firewall_rule) | resource |
| [azurerm_synapse_firewall_rule.allow_all_azure](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_firewall_rule) | resource |
| [azurerm_synapse_managed_private_endpoint.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_managed_private_endpoint) | resource |
| [azurerm_synapse_role_assignment.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_role_assignment) | resource |
| [azurerm_synapse_spark_pool.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_spark_pool) | resource |
| [azurerm_synapse_sql_pool.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_sql_pool) | resource |
| [azurerm_synapse_workspace.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_workspace) | resource |
| [azurerm_synapse_workspace_aad_admin.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_workspace_aad_admin) | resource |
| [null_resource.synapse_managed_private_endpoint_data_lake_approval](https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource) | resource |
| [random_password.synapse_sql_administrator_password](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/password) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [time_sleep.synapse_managed_private_endpoint_delay](https://registry.terraform.io/providers/hashicorp/time/latest/docs/resources/sleep) | resource |
| [azurerm_client_config.current](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/client_config) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data_lake_account_tier"></a> [data\_lake\_account\_tier](#input\_data\_lake\_account\_tier) | The tier of the Synapse data lake Storage Account | `string` | `"Standard"` | no |
| <a name="input_data_lake_replication_type"></a> [data\_lake\_replication\_type](#input\_data\_lake\_replication\_type) | The replication type for the Synapse data lake Storage Account | `string` | `"ZRS"` | no |
| <a name="input_data_lake_role_assignments"></a> [data\_lake\_role\_assignments](#input\_data\_lake\_role\_assignments) | The RBAC assignments to be applied to the Synapse data lake Storage Account | `map(string)` | `{}` | no |
| <a name="input_data_lake_storage_containers"></a> [data\_lake\_storage\_containers](#input\_data\_lake\_storage\_containers) | A list of container names to be created in the Synapse data lake Storage Account | `list(string)` | <pre>[<br>  "default"<br>]</pre> | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_id"></a> [key\_vault\_id](#input\_key\_vault\_id) | The ID of the Key Vault used by this module to store and retrieve secrets | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_purview_id"></a> [purview\_id](#input\_purview\_id) | The ID of the Purview account to link with the Synapse Workspace | `string` | `null` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_spark_pool_enabled"></a> [spark\_pool\_enabled](#input\_spark\_pool\_enabled) | Determines whether a Synapse-linked Spark pool should be deployed | `bool` | `false` | no |
| <a name="input_spark_pool_max_node_count"></a> [spark\_pool\_max\_node\_count](#input\_spark\_pool\_max\_node\_count) | The maximum number of nodes the Synapse-linked Spark pool can autoscale to | `number` | `9` | no |
| <a name="input_spark_pool_min_node_count"></a> [spark\_pool\_min\_node\_count](#input\_spark\_pool\_min\_node\_count) | The minimum number of nodes the Synapse-linked Spark pool can autoscale to | `number` | `3` | no |
| <a name="input_spark_pool_node_size"></a> [spark\_pool\_node\_size](#input\_spark\_pool\_node\_size) | The size of nodes comprising the Synapse-linked Spark pool | `string` | `"Small"` | no |
| <a name="input_spark_pool_version"></a> [spark\_pool\_version](#input\_spark\_pool\_version) | The version of Spark running on the Synapse-linked Spark pool | `string` | `"2.4"` | no |
| <a name="input_sql_pool_collation"></a> [sql\_pool\_collation](#input\_sql\_pool\_collation) | The collation of the Synapse-linked dedicated SQL pool | `string` | `"SQL_Latin1_General_CP1_CI_AS"` | no |
| <a name="input_sql_pool_enabled"></a> [sql\_pool\_enabled](#input\_sql\_pool\_enabled) | Determines whether a Synapse-linked dedicated SQL pool should be deployed | `bool` | `false` | no |
| <a name="input_sql_pool_sku_name"></a> [sql\_pool\_sku\_name](#input\_sql\_pool\_sku\_name) | The SKU of the Synapse-linked dedicated SQL pool | `string` | `"DW100c"` | no |
| <a name="input_synapse_aad_administrator"></a> [synapse\_aad\_administrator](#input\_synapse\_aad\_administrator) | A map describing the username and Azure AD object ID for the Syanapse administrator account | `map(string)` | n/a | yes |
| <a name="input_synapse_github_details"></a> [synapse\_github\_details](#input\_synapse\_github\_details) | The GitHub repository details to establish a link with the Synapse Workspace | `map(string)` | `{}` | no |
| <a name="input_synapse_github_enabled"></a> [synapse\_github\_enabled](#input\_synapse\_github\_enabled) | Determines whether a GitHub repository should be linked to the Synapse Workspace | `bool` | `false` | no |
| <a name="input_synapse_private_endpoint_dns_zone_id"></a> [synapse\_private\_endpoint\_dns\_zone\_id](#input\_synapse\_private\_endpoint\_dns\_zone\_id) | The ID of the Private DNS Zone hosting privatelink.azuresynapse.net | `string` | n/a | yes |
| <a name="input_synapse_private_endpoint_subnet_name"></a> [synapse\_private\_endpoint\_subnet\_name](#input\_synapse\_private\_endpoint\_subnet\_name) | The name of the subnet into which Synapse private endpoints should be deployed | `string` | `"SynapseEndpointSubnet"` | no |
| <a name="input_synapse_private_endpoint_vnet_subnets"></a> [synapse\_private\_endpoint\_vnet\_subnets](#input\_synapse\_private\_endpoint\_vnet\_subnets) | A map of subnet names and IDs comprising the linked Virtual Network for private endpoint deployment | `map(string)` | n/a | yes |
| <a name="input_synapse_role_assignments"></a> [synapse\_role\_assignments](#input\_synapse\_role\_assignments) | The Synapse-specific RBAC assignments to be applied to the Synapse Workspace | `map(string)` | `{}` | no |
| <a name="input_synapse_sql_administrator_username"></a> [synapse\_sql\_administrator\_username](#input\_synapse\_sql\_administrator\_username) | The SQL administrator username for the Synapse Workspace | `string` | `"synadmin"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
