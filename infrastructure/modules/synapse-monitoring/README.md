# Synapse Monitoring

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
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.16.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_log_analytics_workspace.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/log_analytics_workspace) | resource |
| [azurerm_monitor_diagnostic_setting.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.key_vault](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.network](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.service_bus_namespace](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse_spark_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse_sql_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data_lake_account_id"></a> [data\_lake\_account\_id](#input\_data\_lake\_account\_id) | The ID of the Data Lake Storage Account from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_id"></a> [key\_vault\_id](#input\_key\_vault\_id) | The ID of the Key Vault from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_log_retention_days"></a> [log\_retention\_days](#input\_log\_retention\_days) | The number of days to retain logs in the Log Analytics Workspace | `number` | `30` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_bus_namespace_id"></a> [service\_bus\_namespace\_id](#input\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_synapse_spark_pool_id"></a> [synapse\_spark\_pool\_id](#input\_synapse\_spark\_pool\_id) | The ID of the Synapse Spark Pool from which to collect diagnostic logs | `string` | `null` | no |
| <a name="input_synapse_sql_pool_id"></a> [synapse\_sql\_pool\_id](#input\_synapse\_sql\_pool\_id) | The ID of the Synapse Dedicated SQL Pool from which to collect diagnostic logs | `string` | `null` | no |
| <a name="input_synapse_vnet_id"></a> [synapse\_vnet\_id](#input\_synapse\_vnet\_id) | The ID of the Synapse Virtual network from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_synapse_workspace_id"></a> [synapse\_workspace\_id](#input\_synapse\_workspace\_id) | The ID of the Synapse Workspace from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
