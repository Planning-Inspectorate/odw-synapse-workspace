resource "azurerm_private_endpoint" "key_vault" {
  name                = "pins-pe-${azurerm_key_vault.synapse.name}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "keyVaultDnsZone"
    private_dns_zone_ids = [var.key_vault_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "keyVault"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_key_vault.synapse.id
    subresource_names              = ["vault"]
  }

  tags = local.tags
}
