resource "azurerm_private_endpoint" "data_lake" {
  name                = "pins-pe-${azurerm_storage_account.synapse.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "dataLakeDnsZone"
    private_dns_zone_ids = [var.data_lake_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "dataLakeDfs"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_storage_account.synapse.id
    subresource_names = [
      "dfs",
      "dfs_secondary"
    ]
  }

  tags = local.tags
}
