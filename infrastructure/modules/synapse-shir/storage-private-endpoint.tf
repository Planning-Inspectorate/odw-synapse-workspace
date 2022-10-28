resource "azurerm_private_endpoint" "shir_blob" {
  name                = "pins-pe-${azurerm_storage_account.shir.name}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.devops_agent_subnet_name]

  private_dns_zone_group {
    name                 = "blobDnsZone"
    private_dns_zone_ids = [var.blob_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "shirBlob"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_storage_account.shir.id
    subresource_names              = ["blob"]
  }

  tags = local.tags
}
