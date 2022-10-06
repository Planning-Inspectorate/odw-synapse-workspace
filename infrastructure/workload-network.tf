resource "azurerm_resource_group" "network" {
  name     = "pins-rg-network-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "network_failover" {
  name     = "pins-rg-network-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "network_global" {
  name     = "pins-rg-network-${local.resource_suffix_global}"
  location = module.azure_region.location_cli

  tags = local.tags
}

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

resource "azurerm_network_security_group" "nsgs" {
  for_each = module.synapse_network.vnet_subnets

  name                = "pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix}"
  location            = module.azure_region.location_cli
  resource_group_name = azurerm_resource_group.network.name

  tags = local.tags
}

resource "azurerm_network_security_group" "nsgs_failover" {
  for_each = module.synapse_network_failover.vnet_subnets

  name                = "pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix_failover}"
  location            = module.azure_region.paired_location.location_cli
  resource_group_name = azurerm_resource_group.network_failover.name

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

resource "azurerm_subnet_network_security_group_association" "nsgs" {
  for_each = module.synapse_network.vnet_subnets

  network_security_group_id = "${azurerm_resource_group.network.id}/${local.nsg_path}/pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix}"
  subnet_id                 = each.value

  depends_on = [
    azurerm_network_security_group.nsgs
  ]
}

resource "azurerm_subnet_network_security_group_association" "nsgs_failover" {
  for_each = module.synapse_network_failover.vnet_subnets

  network_security_group_id = "${azurerm_resource_group.network_failover.id}/${local.nsg_path}/pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix_failover}"
  subnet_id                 = each.value

  depends_on = [
    azurerm_network_security_group.nsgs_failover
  ]
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
