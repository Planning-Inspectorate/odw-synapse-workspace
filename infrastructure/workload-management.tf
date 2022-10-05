resource "azurerm_resource_group" "data_management" {
  name     = "pins-rg-datamgmt-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "data_management_failover" {
  count = var.failover_deployment ? 1 : 0

  name     = "pins-rg-datamgmt-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "synapse_management" {
  source = "./modules/synapse-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  deploy_purview             = true
  key_vault_role_assignments = var.key_vault_role_assignments

  tags = local.tags
}

module "synapse_management_failover" {
  count = var.failover_deployment ? 1 : 0

  source = "./modules/synapse-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  deploy_purview             = false # Not supported in the UK West region
  key_vault_role_assignments = var.key_vault_role_assignments

  tags = local.tags
}

module "bastion_host" {
  count = var.bastion_host_enabled ? 1 : 0

  source = "./modules/bastion-host"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  bastion_vm_image            = var.bastion_vm_image
  bastion_vm_username         = var.bastion_vm_username
  bastion_vm_size             = var.bastion_vm_size
  key_vault_id                = module.synapse_management.key_vault_id
  network_resource_group_name = azurerm_resource_group.network.name
  synapse_compute_subnet_name = local.compute_subnet_name
  synapse_vnet_subnets        = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_management
  ]

  tags = local.tags
}

module "bastion_host_failover" {
  count = var.bastion_host_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/bastion-host"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  bastion_vm_image            = var.bastion_vm_image
  bastion_vm_username         = var.bastion_vm_username
  bastion_vm_size             = var.bastion_vm_size
  key_vault_id                = module.synapse_management_failover[0].key_vault_id
  network_resource_group_name = azurerm_resource_group.network_failover.name
  synapse_compute_subnet_name = local.compute_subnet_name
  synapse_vnet_subnets        = module.synapse_network_failover.vnet_subnets

  depends_on = [
    module.synapse_network_failover,
    module.synapse_management_failover
  ]

  tags = local.tags
}
