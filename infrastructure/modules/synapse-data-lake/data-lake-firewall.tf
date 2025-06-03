resource "azurerm_storage_account_network_rules" "synapse" {
  storage_account_id         = azurerm_storage_account.synapse.id
  default_action             = "Deny"
  bypass                     = ["AzureServices", "Metrics", "Logging"]
  ip_rules                   = var.firewall_allowed_ip_addresses
  virtual_network_subnet_ids = local.azurerm_synapse_vnet_subnet_ids
}

# read Horizon Subnet Ids
data "azurerm_subnet" "horizon_database" {
  count                = var.external_resource_links_enabled ? 1 : 0
  name                 = var.horizon_integration_config.networking.database_subnet_name
  virtual_network_name = var.horizon_integration_config.networking.vnet_name
  resource_group_name  = var.horizon_integration_config.networking.resource_group_name

  provider = azurerm.horizon
}
