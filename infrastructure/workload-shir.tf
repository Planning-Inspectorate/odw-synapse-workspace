resource "azurerm_resource_group" "shir" {
  name     = "pins-rg-shir-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "shir_failover" {
  count = var.failover_deployment ? 1 : 0

  name     = "pins-rg-shir-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "synapse_shir" {
  source = "./modules/synapse-shir"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.shir.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  blob_private_endpoint_dns_zone_id = azurerm_private_dns_zone.blob.id
  devops_agent_subnet_name          = local.compute_subnet_name
  network_resource_group_name       = azurerm_resource_group.network.name
  synapse_workspace_id              = module.synapse_workspace_private.synapse_workspace_id
  vnet_subnet_ids                   = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_workspace_private
  ]

  tags = local.tags
}

module "synapse_shir_failover" {
  count = var.failover_deployment ? 1 : 0

  source = "./modules/synapse-shir"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.shir_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  blob_private_endpoint_dns_zone_id = azurerm_private_dns_zone.blob.id
  devops_agent_subnet_name          = local.compute_subnet_name
  network_resource_group_name       = azurerm_resource_group.network_failover.name
  synapse_workspace_id              = module.synapse_workspace_private_failover[0].synapse_workspace_id
  vnet_subnet_ids                   = module.synapse_network_failover.vnet_subnets

  depends_on = [
    module.synapse_network_failover,
    module.synapse_workspace_private_failover
  ]

  tags = local.tags
}
