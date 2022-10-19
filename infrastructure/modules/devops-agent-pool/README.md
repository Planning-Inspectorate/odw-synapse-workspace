# Bastion Host
This module provisions an Azure DevOps agent pool into the specified a specified subnet.

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
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 3.27.0 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.4.3 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_azure_region"></a> [azure\_region](#module\_azure\_region) | claranet/regions/azurerm | 5.1.0 |

## Resources

| Name | Type |
|------|------|
| [azurerm_linux_virtual_machine_scale_set.devops_agent_pool](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/linux_virtual_machine_scale_set) | resource |
| [azurerm_resource_group.devops_agents](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [random_password.devops_agent_password](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/password) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_deploy_agent_pool"></a> [deploy\_agent\_pool](#input\_deploy\_agent\_pool) | A switch to determine whether the devops agent VM Scale Set should be deployed | `bool` | `false` | no |
| <a name="input_devops_agent_image_id"></a> [devops\_agent\_image\_id](#input\_devops\_agent\_image\_id) | The ID of the VM Image to use for the devops agent VMs | `string` | `null` | no |
| <a name="input_devops_agent_instances"></a> [devops\_agent\_instances](#input\_devops\_agent\_instances) | The base number of devops agents in the VM Scale Set | `number` | `1` | no |
| <a name="input_devops_agent_subnet_name"></a> [devops\_agent\_subnet\_name](#input\_devops\_agent\_subnet\_name) | The name of the subnet into which the devops agent VM Scale Set will be deployed | `string` | n/a | yes |
| <a name="input_devops_agent_username"></a> [devops\_agent\_username](#input\_devops\_agent\_username) | The username of the devops agent local account | `string` | `"agent_user"` | no |
| <a name="input_devops_agent_vm_sku"></a> [devops\_agent\_vm\_sku](#input\_devops\_agent\_vm\_sku) | The size of the devops agent VMs to be deployed | `string` | `"Standard_D2ds_v5"` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment in which resources will be deployed | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The short-format Azure region into which resources will be deployed | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The name of the resource group into which resources will be deployed | `string` | n/a | yes |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | The short-format name of the overarching service being deployed | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | A collection of tags to assign to taggable resources | `map(string)` | `{}` | no |
| <a name="input_vnet_subnet_ids"></a> [vnet\_subnet\_ids](#input\_vnet\_subnet\_ids) | A map of subnet names and IDs comprising the linked Virtual Network | `map(string)` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_resource_group_name"></a> [resource\_group\_name](#output\_resource\_group\_name) | The name of the resource group deployed in this module |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
