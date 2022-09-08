# Synapse Monitoring
This module deploys a Log Analytics Workspace and enables diagnostic logging for several resource types including Synapse Workspace.

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage
The below module definition provides an example of usage. This module is designed to depend on the outputs from the associated `synapse_workspace_private`, `synapse_ingestion`, and `synapse_network` modules. These associated modules provision the Synapse Workspace and associated infrastructure, the IDs of which are used in this module to enable diagnostic logging.

```
module "synapse_monitoring" {
  source = "./modules/synapse-monitoring"

  environment         = "dev"
  resource_group_name = azurerm_resource_group.monitoring.name
  location            = module.azure_region.location_cli
  service_name        = "odw"

  data_lake_account_id     = module.synapse_workspace_private.data_lake_account_id
  key_vault_id             = module.synapse_workspace_private.key_vault_id
  service_bus_namespace_id = module.synapse_ingestion.service_bus_namespace_id
  spark_pool_enabled       = true
  sql_pool_enabled         = true
  synapse_spark_pool_id    = module.synapse_workspace_private.synapse_spark_pool_id
  synapse_sql_pool_id      = module.synapse_workspace_private.synapse_sql_pool_id
  synapse_workspace_id     = module.synapse_workspace_private.synapse_workspace_id
  synapse_vnet_id          = module.synapse_network.vnet_id

  depends_on = [
    module.synapse_ingestion,
    module.synapse_workspace_private
  ]

  tags = local.tags
}
```

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.20.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_log_analytics_storage_insights.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/log_analytics_storage_insights) | resource |
| [azurerm_log_analytics_workspace.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/log_analytics_workspace) | resource |
| [azurerm_monitor_action_group.platform_alerts](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_action_group) | resource |
| [azurerm_monitor_action_group.synapse_alerts](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_action_group) | resource |
| [azurerm_monitor_activity_log_alert.data_lake_deleted](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_activity_log_alert) | resource |
| [azurerm_monitor_activity_log_alert.data_lake_resource_health](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_activity_log_alert) | resource |
| [azurerm_monitor_activity_log_alert.synapse_workspace_deleted](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_activity_log_alert) | resource |
| [azurerm_monitor_activity_log_alert.synapse_workspace_resource_health](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_activity_log_alert) | resource |
| [azurerm_monitor_diagnostic_setting.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.key_vault](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.network](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.service_bus_namespace](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse_spark_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_diagnostic_setting.synapse_sql_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting) | resource |
| [azurerm_monitor_metric_alert.data_lake_capacity](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_metric_alert) | resource |
| [azurerm_monitor_metric_alert.data_lake_latency](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_metric_alert) | resource |
| [azurerm_monitor_metric_alert.synapse_pipeline_runs_failed](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_metric_alert) | resource |
| [azurerm_key_vault_secret.data_lake_storage_account_key](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/key_vault_secret) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_alert_group_platform_enabled"></a> [alert\_group\_platform\_enabled](#input\_alert\_group\_platform\_enabled) | Determines whether the alert group for platform alerts is enabled | `bool` | `false` | no |
| <a name="input_alert_group_platform_recipients"></a> [alert\_group\_platform\_recipients](#input\_alert\_group\_platform\_recipients) | A list of email recipients to recieve platform alerts | `list(string)` | `[]` | no |
| <a name="input_alert_group_synapse_enabled"></a> [alert\_group\_synapse\_enabled](#input\_alert\_group\_synapse\_enabled) | Determines whether the alert group for Synapse alerts is enabled | `bool` | `false` | no |
| <a name="input_alert_group_synapse_recipients"></a> [alert\_group\_synapse\_recipients](#input\_alert\_group\_synapse\_recipients) | A list of email recipients to recieve Synapse alerts | `list(string)` | `[]` | no |
| <a name="input_alert_threshold_data_lake_capacity_bytes"></a> [alert\_threshold\_data\_lake\_capacity\_bytes](#input\_alert\_threshold\_data\_lake\_capacity\_bytes) | The threshold at which to trigger an alert for exceeding Data Lake capacity in bytes | `number` | `1099511627776` | no |
| <a name="input_data_lake_account_id"></a> [data\_lake\_account\_id](#input\_data\_lake\_account\_id) | The ID of the Data Lake Storage Account from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_id"></a> [key\_vault\_id](#input\_key\_vault\_id) | The ID of the Key Vault from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_log_retention_days"></a> [log\_retention\_days](#input\_log\_retention\_days) | The number of days to retain logs in the Log Analytics Workspace | `number` | `30` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_bus_namespace_id"></a> [service\_bus\_namespace\_id](#input\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_spark_pool_enabled"></a> [spark\_pool\_enabled](#input\_spark\_pool\_enabled) | Determines whether a Synapse-linked Spark pool is deployed and should be monitored | `bool` | `false` | no |
| <a name="input_sql_pool_enabled"></a> [sql\_pool\_enabled](#input\_sql\_pool\_enabled) | Determines whether a Synapse-linked dedicated SQL pool is deployed and should be monitored | `bool` | `false` | no |
| <a name="input_synapse_spark_pool_id"></a> [synapse\_spark\_pool\_id](#input\_synapse\_spark\_pool\_id) | The ID of the Synapse Spark Pool from which to collect diagnostic logs | `string` | `null` | no |
| <a name="input_synapse_sql_pool_id"></a> [synapse\_sql\_pool\_id](#input\_synapse\_sql\_pool\_id) | The ID of the Synapse Dedicated SQL Pool from which to collect diagnostic logs | `string` | `null` | no |
| <a name="input_synapse_vnet_id"></a> [synapse\_vnet\_id](#input\_synapse\_vnet\_id) | The ID of the Synapse Virtual network from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_synapse_workspace_id"></a> [synapse\_workspace\_id](#input\_synapse\_workspace\_id) | The ID of the Synapse Workspace from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
