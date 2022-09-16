module "synapse_network" {
  source = "./modules/synapse-network"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.network.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  network_watcher_enabled = var.network_watcher_enabled
  vnet_base_cidr_block    = var.vnet_base_cidr_block
  vnet_subnets            = var.vnet_subnets

  tags = local.tags
}

module "synapse_network_failover" {
  source = "./modules/synapse-network"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.network_failover.name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  network_watcher_enabled = var.network_watcher_enabled
  vnet_base_cidr_block    = var.vnet_base_cidr_block_failover
  vnet_subnets            = var.vnet_subnets

  tags = local.tags
}

resource "azurerm_private_dns_zone" "data_lake" {
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = azurerm_resource_group.network_global.name

  tags = local.tags
}

resource "azurerm_private_dns_zone" "synapse" {
  name                = "privatelink.azuresynapse.net"
  resource_group_name = azurerm_resource_group.network_global.name

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "data_lake" {
  name                  = "dfs-${module.synapse_network.vnet_name}"
  resource_group_name   = azurerm_resource_group.network_global.name
  private_dns_zone_name = azurerm_private_dns_zone.data_lake.name
  virtual_network_id    = module.synapse_network.vnet_id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "data_lake_failover" {
  name                  = "dfs-${module.synapse_network_failover.vnet_name}"
  resource_group_name   = azurerm_resource_group.network_global.name
  private_dns_zone_name = azurerm_private_dns_zone.data_lake.name
  virtual_network_id    = module.synapse_network_failover.vnet_id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "synapse" {
  name                  = "azuresynapse-${module.synapse_network.vnet_name}"
  resource_group_name   = azurerm_resource_group.network_global.name
  private_dns_zone_name = azurerm_private_dns_zone.synapse.name
  virtual_network_id    = module.synapse_network.vnet_id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "synapse_failover" {
  name                  = "azuresynapse-${module.synapse_network_failover.vnet_name}"
  resource_group_name   = azurerm_resource_group.network_global.name
  private_dns_zone_name = azurerm_private_dns_zone.synapse.name
  virtual_network_id    = module.synapse_network_failover.vnet_id

  tags = local.tags
}

resource "azurerm_virtual_network_peering" "pri_sec" {
  name                      = "peer-${module.synapse_network.vnet_name}-${module.synapse_network_failover.vnet_name}"
  resource_group_name       = azurerm_resource_group.network.name
  virtual_network_name      = module.synapse_network.vnet_name
  remote_virtual_network_id = module.synapse_network_failover.vnet_id
}

resource "azurerm_virtual_network_peering" "sec_pri" {
  name                      = "peer-${module.synapse_network_failover.vnet_name}-${module.synapse_network.vnet_name}"
  resource_group_name       = azurerm_resource_group.network_failover.name
  virtual_network_name      = module.synapse_network_failover.vnet_name
  remote_virtual_network_id = module.synapse_network.vnet_id
}
