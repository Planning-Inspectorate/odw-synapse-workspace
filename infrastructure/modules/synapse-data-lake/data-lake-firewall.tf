resource "azurerm_storage_account_network_rules" "synapse" {
  storage_account_id = azurerm_storage_account.synapse.id
  default_action     = "Deny"
  bypass             = ["AzureServices", "Metrics", "Logging"]
  ip_rules           = var.firewall_allowed_ip_addresses
  virtual_network_subnet_ids = [
    var.vnet_subnet_ids[var.devops_agent_subnet_name],
    var.vnet_subnet_ids_failover[var.devops_agent_subnet_name]
  ]
}
