# Synapse Data Lake

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
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.22.0 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.4.3 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_management_lock.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/management_lock) | resource |
| [azurerm_private_endpoint.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/private_endpoint) | resource |
| [azurerm_role_assignment.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.terraform](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_data_lake_gen2_filesystem.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_data_lake_gen2_filesystem) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [azurerm_client_config.current](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/client_config) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data_lake_account_tier"></a> [data\_lake\_account\_tier](#input\_data\_lake\_account\_tier) | The tier of the Synapse data lake Storage Account | `string` | `"Standard"` | no |
| <a name="input_data_lake_private_endpoint_dns_zone_id"></a> [data\_lake\_private\_endpoint\_dns\_zone\_id](#input\_data\_lake\_private\_endpoint\_dns\_zone\_id) | The ID of the Private DNS Zone hosting privatelink.dfs.core.windows.net | `string` | n/a | yes |
| <a name="input_data_lake_replication_type"></a> [data\_lake\_replication\_type](#input\_data\_lake\_replication\_type) | The replication type for the Synapse data lake Storage Account | `string` | `"ZRS"` | no |
| <a name="input_data_lake_retention_days"></a> [data\_lake\_retention\_days](#input\_data\_lake\_retention\_days) | The number of days blob and queue data will be retained for upon deletion | `number` | `7` | no |
| <a name="input_data_lake_role_assignments"></a> [data\_lake\_role\_assignments](#input\_data\_lake\_role\_assignments) | An object mapping RBAC roles to principal IDs for the data lake Storage Account | `map(list(string))` | `{}` | no |
| <a name="input_data_lake_storage_containers"></a> [data\_lake\_storage\_containers](#input\_data\_lake\_storage\_containers) | A list of container names to be created in the Synapse data lake Storage Account | `list(string)` | <pre>[<br>  "default"<br>]</pre> | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_network_resource_group_name"></a> [network\_resource\_group\_name](#input\_network\_resource\_group\_name) | The name of the resource group into which private endpoints will be deployed | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_synapse_private_endpoint_subnet_name"></a> [synapse\_private\_endpoint\_subnet\_name](#input\_synapse\_private\_endpoint\_subnet\_name) | The name of the subnet into which Synapse private endpoints should be deployed | `string` | `"SynapseEndpointSubnet"` | no |
| <a name="input_synapse_private_endpoint_vnet_subnets"></a> [synapse\_private\_endpoint\_vnet\_subnets](#input\_synapse\_private\_endpoint\_vnet\_subnets) | A map of subnet names and IDs comprising the linked Virtual Network for private endpoint deployment | `map(string)` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_data_lake_account_id"></a> [data\_lake\_account\_id](#output\_data\_lake\_account\_id) | The ID of the Data Lake Storage Account |
| <a name="output_data_lake_account_name"></a> [data\_lake\_account\_name](#output\_data\_lake\_account\_name) | The name of the Data Lake Storage Account |
| <a name="output_data_lake_dfs_endpoint"></a> [data\_lake\_dfs\_endpoint](#output\_data\_lake\_dfs\_endpoint) | The DFS endpoint URL of the Data Lake Storage Account |
| <a name="output_data_lake_filesystem_id"></a> [data\_lake\_filesystem\_id](#output\_data\_lake\_filesystem\_id) | The ID of the Data Lake Gen2 filesystem |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
