module "synapse_management" {
  source = "./modules/synapse-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

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
  synapse_compute_subnet_name = local.compute_subnet_name
  synapse_vnet_subnets        = var.failover_deployment ? module.synapse_network_failover.vnet_subnets : module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_network_failover,
    module.synapse_management
  ]

  tags = local.tags
}
