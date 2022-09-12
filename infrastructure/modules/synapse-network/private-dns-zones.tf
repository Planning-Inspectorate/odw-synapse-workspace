resource "azurerm_private_dns_zone" "data_lake" {
  name                = "dfs.core.windows.net"
  resource_group_name = var.resource_group_name

  tags = local.tags
}

resource "azurerm_private_dns_zone" "synapse" {
  name                = "privatelink.azuresynapse.net"
  resource_group_name = var.resource_group_name

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "data_lake" {
  name                  = "dfs-${azurerm_virtual_network.synapse.name}"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.data_lake.name
  virtual_network_id    = azurerm_virtual_network.synapse.id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "synapse" {
  name                  = "azuresynapse-${azurerm_virtual_network.synapse.name}"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.synapse.name
  virtual_network_id    = azurerm_virtual_network.synapse.id

  tags = local.tags
}
