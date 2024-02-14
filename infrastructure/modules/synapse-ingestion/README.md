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
| <a name="provider_azuread"></a> [azuread](#provider\_azuread) | 2.47.0 |
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
| [azurerm_role_assignment.service_bus_subscription_role_assignments](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_pri_service_bus_rx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_pri_service_bus_tx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_sec_service_bus_rx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.synapse_sec_service_bus_tx](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_servicebus_namespace.synapse](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_namespace) | resource |
| [azurerm_servicebus_namespace_disaster_recovery_config.failover](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_namespace_disaster_recovery_config) | resource |
| [azurerm_servicebus_subscription.topic_subscriptions](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_subscription) | resource |
| [azurerm_servicebus_topic.topics](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/servicebus_topic) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [azuread_group.groups](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/data-sources/group) | data source |
| [azuread_service_principal.service_principals](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/data-sources/service_principal) | data source |
| [azuread_user.users](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/data-sources/user) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_failover_namespace"></a> [failover\_namespace](#input\_failover\_namespace) | Determines whether the Service Bus Namespace will be configured as a failover instance | `bool` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_primary_service_bus_namespace_id"></a> [primary\_service\_bus\_namespace\_id](#input\_primary\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace to replicate from if failover\_namespace is true | `string` | `null` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_bus_failover_enabled"></a> [service\_bus\_failover\_enabled](#input\_service\_bus\_failover\_enabled) | Determines whether the Service Bus Namespace will be provisioned with the Premium SKU for failover | `bool` | `false` | no |
| <a name="input_service_bus_role_assignments"></a> [service\_bus\_role\_assignments](#input\_service\_bus\_role\_assignments) | "A map of maps containing the role assignments to be created in the Service Bus Namespace.<br>{<br>  "role\_assignment\_name" = {<br>    users = [<br>      "user\_principal\_name"<br>    ]<br>    groups = [<br>      "group\_principal\_name"<br>    ]<br>    service\_principals = [<br>      "service\_principal\_name"<br>    ]<br>  }<br>} | `any` | `{}` | no |
| <a name="input_service_bus_topics_and_subscriptions"></a> [service\_bus\_topics\_and\_subscriptions](#input\_service\_bus\_topics\_and\_subscriptions) | "A map of maps containing the configuration for Service Bus Topics and Subscriptions to be created in the Service Bus Namespace.<br>  {<br>  name                                    = "topic\_name"<br>  status                                  = "Active"<br>  auto\_delete\_on\_idle                     = "P10675199DT2H48M5.4775807S"<br>  default\_message\_ttl                     = "P14D"<br>  duplicate\_detection\_history\_time\_window = "P7D"<br>  enable\_batched\_operations               = true<br>  enable\_express                          = false<br>  enable\_partitioning                     = false<br>  max\_size\_in\_megabytes                   = 1024<br>  requires\_duplicate\_detection            = true<br>  support\_ordering                        = false<br>  subscriptions                           = {<br>    "subscription\_name" { =<br>      status                                    = "Active"<br>      max\_delivery\_count                        = 1<br>      auto\_delete\_on\_idle                       = "PT5M"<br>      default\_message\_ttl                       = "P14D"<br>      lock\_duration                             = "P0DT0H1M0S"<br>      dead\_lettering\_on\_message\_expiration      = false<br>      dead\_lettering\_on\_filter\_evaluation\_error = true<br>      enable\_batched\_operations                 = false<br>      requires\_session                          = false<br>      forward\_to                                = ""<br>      role\_assignments                          = {<br>        "role\_name" = {<br>          users = []<br>          groups = []<br>          service\_principals = []<br>        }<br>      }<br>    }<br>  }<br>}" | `any` | `{}` | no |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_synapse_workspace_failover_principal_id"></a> [synapse\_workspace\_failover\_principal\_id](#input\_synapse\_workspace\_failover\_principal\_id) | The managed identity for the failover Synapse Workspace | `string` | `null` | no |
| <a name="input_synapse_workspace_principal_id"></a> [synapse\_workspace\_principal\_id](#input\_synapse\_workspace\_principal\_id) | The managed identity for the Synapse Workspace | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_service_bus_namespace_id"></a> [service\_bus\_namespace\_id](#output\_service\_bus\_namespace\_id) | The ID of the Service Bus Namespace deployed in this module |
| <a name="output_service_bus_namespace_name"></a> [service\_bus\_namespace\_name](#output\_service\_bus\_namespace\_name) | The name of the Service Bus Namespace deployed in this module |
| <a name="output_service_bus_primary_connection_string"></a> [service\_bus\_primary\_connection\_string](#output\_service\_bus\_primary\_connection\_string) | The name of the Service Bus Namespace deployed in this module |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
