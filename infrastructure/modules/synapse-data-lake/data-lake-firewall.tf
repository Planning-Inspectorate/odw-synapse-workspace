resource "azurerm_storage_account_network_rules" "synapse" {
  storage_account_id = azurerm_storage_account.synapse.id
  default_action     = "Deny"
  bypass             = ["AzureServices", "Metrics", "Logging"]
  ip_rules           = var.firewall_allowed_ip_addresses
  virtual_network_subnet_ids = [
    var.vnet_subnet_ids[var.devops_agent_subnet_name],
    var.vnet_subnet_ids_failover[var.devops_agent_subnet_name],
    var.vnet_subnet_ids[var.function_app_subnet_name],
    var.vnet_subnet_ids_failover[var.function_app_subnet_name],
    data.azurerm_subnet.horizon_database.id
  ]
}

# read Horizon Subnet Ids
data "azurerm_subnet" "horizon_database" {
  name                 = var.horizon_integration_config.networking.database_subnet_name
  virtual_network_name = var.horizon_integration_config.networking.vnet_name
  resource_group_name  = var.horizon_integration_config.networking.resource_group_name

  provider = azurerm.horizon
}
