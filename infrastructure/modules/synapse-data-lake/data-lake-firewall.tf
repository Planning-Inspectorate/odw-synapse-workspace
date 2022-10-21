resource "azurerm_storage_account_network_rules" "synapse" {
  storage_account_id = azurerm_storage_account.synapse.id
  default_action     = "Deny"
  bypass             = ["AzureServices", "Metrics", "Logging"]
  ip_rules           = var.data_lake_allowed_ip_addresses
}
