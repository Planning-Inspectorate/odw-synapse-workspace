# Synapse Ingestion
This module provisions a Service Bus Namespace and Topic for publishing data to be consumed by external subscribers.

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage
The below module definition provides an example of usage.

```
module "synapse_ingestion" {
  source = "./modules/synapse-ingestion"

  environment         = "dev"
  resource_group_name = azurerm_resource_group.ingestion.name
  location            = module.azure_region.location_cli
  service_name        = "odw"

  tags = local.tags
}
```

| :scroll: Note |
|----------|
| The resources in this module are currently hardcoded and as such this module has no dependencies. |

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.39.1 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.4.3 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_role_assignment.service_bus](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_pri_service_bus_rx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_pri_service_bus_tx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_sec_service_bus_rx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_sec_service_bus_tx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_servicebus_namespace.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_namespace) | resource |
| [azurerm_servicebus_namespace_disaster_recovery_config.failover](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_namespace_disaster_recovery_config) | resource |
| [azurerm_servicebus_subscription.topic_subscriptions](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_subscription) | resource |
| [azurerm_servicebus_topic.topics](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_topic) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_failover_namespace"></a> [failover\_namespace](#input\_failover\_namespace) | Determines whether the Service Bus Namespace will be configured as a failover instance | `bool` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_primary_service_bus_namespace_id"></a> [primary\_service\_bus\_namespace\_id](#input\_primary\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace to replicate from if failover\_namespace is true | `string` | `null` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_bus_failover_enabled"></a> [service\_bus\_failover\_enabled](#input\_service\_bus\_failover\_enabled) | Determines whether the Service Bus Namespace will be provisioned with the Premium SKU for failover | `bool` | `false` | no |
| <a name="input_service_bus_role_assignments"></a> [service\_bus\_role\_assignments](#input\_service\_bus\_role\_assignments) | An object mapping RBAC roles to principal IDs for the service bus | `map(list(string))` | `{}` | no |
| <a name="input_service_bus_topics_and_subscriptions"></a> [service\_bus\_topics\_and\_subscriptions](#input\_service\_bus\_topics\_and\_subscriptions) | An object mapping Service Bus Topics to a list of Subscription names | `map(list(string))` | `{}` | no |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_synapse_workspace_failover_principal_id"></a> [synapse\_workspace\_failover\_principal\_id](#input\_synapse\_workspace\_failover\_principal\_id) | The managed identity for the failover Synapse Workspace | `string` | `null` | no |
| <a name="input_synapse_workspace_principal_id"></a> [synapse\_workspace\_principal\_id](#input\_synapse\_workspace\_principal\_id) | The managed identity for the Synapse Workspace | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_service_bus_namespace_id"></a> [service\_bus\_namespace\_id](#output\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace deployed in this module |
| <a name="output_service_bus_namespace_name"></a> [service\_bus\_namespace\_name](#output\_service\_bus\_namespace\_name) | The name of the Service Bus Namespace deployed in this module |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
