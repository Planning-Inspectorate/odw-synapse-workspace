# PINS Synapse Workload

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.1.6, < 2.0.0 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | ~> 3.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.13.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |
| <a name="module_synapse_common"></a> [synapse\_common](#module\_synapse\_common) | ./modules/synapse-common | n/a |
| <a name="module_synapse_management"></a> [synapse\_management](#module\_synapse\_management) | ./modules/synapse-management | n/a |
| <a name="module_synapse_workspace_private"></a> [synapse\_workspace\_private](#module\_synapse\_workspace\_private) | ./modules/synapse-workspace-private | n/a |

## Resources

| Name | Type |
|------|------|
| [azurerm_resource_group.data](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_resource_group.data_management](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data_lake_account_tier"></a> [data\_lake\_account\_tier](#input\_data\_lake\_account\_tier) | The tier of the Synapse data lake Storage Account | `string` | `"Standard"` | no |
| <a name="input_data_lake_replication_type"></a> [data\_lake\_replication\_type](#input\_data\_lake\_replication\_type) | The replication type for the Synapse data lake Storage Account | `string` | `"ZRS"` | no |
| <a name="input_data_lake_role_assignments"></a> [data\_lake\_role\_assignments](#input\_data\_lake\_role\_assignments) | The RBAC assignments to be applied to the Synapse data lake Storage Account | `map(string)` | `{}` | no |
| <a name="input_data_lake_storage_containers"></a> [data\_lake\_storage\_containers](#input\_data\_lake\_storage\_containers) | A list of container names to be created in the Synapse data lake Storage Account | `list(string)` | <pre>[<br>  "default"<br>]</pre> | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_role_assignments"></a> [key\_vault\_role\_assignments](#input\_key\_vault\_role\_assignments) | The RBAC assignments to be applied to the Key Vault | `map(string)` | `{}` | no |
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
| <a name="input_synapse_github_details"></a> [synapse\_github\_details](#input\_synapse\_github\_details) | The GitHub repository details to establish a link with the Synapse Workspace | `map(string)` | `{}` | no |
| <a name="input_synapse_github_enabled"></a> [synapse\_github\_enabled](#input\_synapse\_github\_enabled) | Determines whether a GitHub repository should be linked to the Synapse Workspace | `bool` | `false` | no |
| <a name="input_synapse_role_assignments"></a> [synapse\_role\_assignments](#input\_synapse\_role\_assignments) | The Synapse-specific RBAC assignments to be applied to the Synapse Workspace | `map(string)` | `{}` | no |
| <a name="input_synapse_sql_administrator_username"></a> [synapse\_sql\_administrator\_username](#input\_synapse\_sql\_administrator\_username) | The SQL administrator username for the Synapse Workspace | `string` | `"synadmin"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |
| <a name="input_vnet_base_cidr_block"></a> [vnet\_base\_cidr\_block](#input\_vnet\_base\_cidr\_block) | The base IPv4 range for the Virtual Network in CIDR notation | `string` | `"10.90.0.0/24"` | no |
| <a name="input_vnet_subnets"></a> [vnet\_subnets](#input\_vnet\_subnets) | A collection of subnet definitions used to logically partition the Virtual Network | `list(map(string))` | <pre>[<br>  {<br>    "name": "ManagementSubnet",<br>    "new_bits": 2<br>  },<br>  {<br>    "name": "SynapseEndpointSubnet",<br>    "new_bits": 2<br>  },<br>  {<br>    "name": null,<br>    "new_bits": 2<br>  },<br>  {<br>    "name": null,<br>    "new_bits": 2<br>  }<br>]</pre> | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
