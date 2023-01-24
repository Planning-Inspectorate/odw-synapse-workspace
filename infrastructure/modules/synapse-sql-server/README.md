# SQL Server
This module provisions an Azure SQL Server.

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage
The below module definition provides an example of usage.

```
module "sql_server" {
  count = var.sql_server_enabled ? 1 : 0

  source = "./modules/sql-server"

  environment         = "dev"
  resource_group_name = azurerm_resource_group.sql_server.name
  location            = "uk-south"
  service_name        = "odw"

  sql_server_aad_administrator = {
    username  = "sql-admins"
    object_id = "00000000-0000-0000-0000-000000000000"
  }

  depends_on = [
    module.synapse_workspace_private
  ]
}
```

| :scroll: Note |
|----------|
| The use of `count` allows the module to be optionally deployed. |

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
| [azurerm_key_vault_secret.sql_server_administrator_password](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_key_vault_secret.sql_server_administrator_username](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_mssql_server.sql_server](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/mssql_server) | resource |
| [azurerm_mssql_server_extended_auditing_policy.sql_server_auditing](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/mssql_server_extended_auditing_policy) | resource |
| [azurerm_role_assignment.sql_server_auditing](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.sql_server_auditing](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_account_network_rules.sql_server_auditing](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account_network_rules) | resource |
| [azurerm_synapse_managed_private_endpoint.data_lake](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/synapse_managed_private_endpoint) | resource |
| [random_password.sql_server_administrator_password](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/password) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_devops_agent_subnet_name"></a> [devops\_agent\_subnet\_name](#input\_devops\_agent\_subnet\_name) | The name of the subnet into which the devops agents will be deployed | `string` | `"ComputeSubnet"` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_firewall_allowed_ip_addresses"></a> [firewall\_allowed\_ip\_addresses](#input\_firewall\_allowed\_ip\_addresses) | A list of CIDR ranges to be permitted access to the data lake Storage Account | `list(string)` | `[]` | no |
| <a name="input_key_vault_id"></a> [key\_vault\_id](#input\_key\_vault\_id) | The ID of the Key Vault to use for secret storage | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_sql_server_aad_administrator"></a> [sql\_server\_aad\_administrator](#input\_sql\_server\_aad\_administrator) | A map describing the username and Azure AD object ID for the SQL administrator account | `map(string)` | n/a | yes |
| <a name="input_sql_server_administrator_username"></a> [sql\_server\_administrator\_username](#input\_sql\_server\_administrator\_username) | The SQL administrator username for the SQL Server | `string` | `"sqladmin"` | no |
| <a name="input_synapse_workspace_id"></a> [synapse\_workspace\_id](#input\_synapse\_workspace\_id) | The ID of the Synapse Workspace from which to collect diagnostic logs | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |
| <a name="input_vnet_subnet_ids"></a> [vnet\_subnet\_ids](#input\_vnet\_subnet\_ids) | A map of subnet names and IDs comprising the linked Virtual Network | `map(string)` | n/a | yes |
| <a name="input_vnet_subnet_ids_failover"></a> [vnet\_subnet\_ids\_failover](#input\_vnet\_subnet\_ids\_failover) | A map of subnet names and IDs comprising the linked Virtual Network | `map(string)` | n/a | yes |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
