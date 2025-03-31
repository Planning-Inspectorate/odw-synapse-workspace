resource "azurerm_private_endpoint" "data_lake" {
  name                = "pins-pe-${azurerm_storage_account.synapse.name}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "dataLakeDnsZone"
    private_dns_zone_ids = [var.data_lake_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "dataLakeDfs"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_storage_account.synapse.id
    subresource_names              = ["dfs"]
  }

  tags = local.tags
}

# private endpoints in tooling
locals {
  storage_zones = ["blob", "dfs", "file", "queue", "table", "web"]
}

resource "azurerm_private_endpoint" "tooling_data_lake" {
  for_each = toset(local.storage_zones)

  name                = "pins-pe-syn-${each.key}-tooling-${local.resource_suffix}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "storagePrivateDnsZone${each.key}"
    private_dns_zone_ids = [var.tooling_config.storage_private_dns_zone_id[each.key]]
  }

  private_service_connection {
    name                           = "storagePrivateServiceConnection${each.key}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_storage_account.synapse.id
    subresource_names              = [each.key]
  }

  tags = local.tags
}