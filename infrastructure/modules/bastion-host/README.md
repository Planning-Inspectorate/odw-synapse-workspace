# Bastion Host
This module enables Azure Bastion and provisions a Windows virtual machine as a jumpbox into the internal virtual network.

### Table of Contents
1. [Usage](#usage)
2. [Requirements](#requirements)
3. [Providers](#Providers)
4. [Inputs](#inputs)
5. [Outputs](#outputs)

## Usage
The below module definition provides an example of usage. This module is designed to depend on the outputs from the associated `synapse_network` and `synapse_management` modules. These associate modules provision the Synapse virtual network and management Key Vault respectively, in-line with current Cloud Adoption Framework best-practice architecture.
```
module "bastion_host" {
  count = var.bastion_host_enabled ? 1 : 0

  source = "./modules/bastion-host"

  environment         = "dev"
  resource_group_name = azurerm_resource_group.data_management.name
  location            = "uks"
  service_name        = "odw"

  bastion_vm_username         = "basadmin"
  bastion_vm_size             = "Standard_F2s_v2"
  key_vault_id                = module.synapse_management.key_vault_id
  synapse_compute_subnet_name = "ComputeSubnet"
  synapse_vnet_subnets        = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_management
  ]

  tags = local.tags
}
```

| :scroll: Note |
|----------|
| The use of `count` allows the module to be optionally deployed. Azure Bastion is an expensive service and should be deployed and removed as required for development or troubleshooting. |

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
| [azurerm_bastion_host.bastion_host](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/bastion_host) | resource |
| [azurerm_key_vault_secret.bastion_vm_admin_password](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_key_vault_secret.bastion_vm_admin_username](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault_secret) | resource |
| [azurerm_network_interface.jumpbox](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_interface) | resource |
| [azurerm_network_security_group.bastion_host](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_group) | resource |
| [azurerm_network_security_rule.bastion_allow_azure_cloud_out](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_gateway_manager_in](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_host_comms_in](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_host_comms_out](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_http_in](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_load_balancer_in](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_session_info_out](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_network_security_rule.bastion_allow_ssh_rdp_out](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/network_security_rule) | resource |
| [azurerm_public_ip.bastion_host](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/public_ip) | resource |
| [azurerm_windows_virtual_machine.jumpbox](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/windows_virtual_machine) | resource |
| [random_password.bastion_vm_admin_password](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/password) | resource |
| [random_string.unique_id](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_bastion_vm_image"></a> [bastion\_vm\_image](#input\_bastion\_vm\_image) | An object describing the image specification to use for the Bastion jumpbox VM | `map(string)` | <pre>{<br>  "offer": "windows-11",<br>  "publisher": "MicrosoftWindowsDesktop",<br>  "sku": "win11-21h2-ent",<br>  "version": "latest"<br>}</pre> | no |
| <a name="input_bastion_vm_size"></a> [bastion\_vm\_size](#input\_bastion\_vm\_size) | The size of the Bastion jumpbox VM to be deployed | `string` | `"Standard_F2s_v2"` | no |
| <a name="input_bastion_vm_username"></a> [bastion\_vm\_username](#input\_bastion\_vm\_username) | The Windows administrator username for the Bastion jumpbox VM | `string` | `"basadmin"` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_key_vault_id"></a> [key\_vault\_id](#input\_key\_vault\_id) | The ID of the Key Vault to use for secret storage | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_network_resource_group_name"></a> [network\_resource\_group\_name](#input\_network\_resource\_group\_name) | The name of the resource group into which private endpoints will be deployed | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_synapse_compute_subnet_name"></a> [synapse\_compute\_subnet\_name](#input\_synapse\_compute\_subnet\_name) | The name of the subnet into which the Bastion jumpbox VM should be deployed | `string` | `"ComputeSubnet"` | no |
| <a name="input_synapse_vnet_subnets"></a> [synapse\_vnet\_subnets](#input\_synapse\_vnet\_subnets) | A map of subnet names and IDs comprising the linked Virtual Network for Bastion host deployment | `map(string)` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
