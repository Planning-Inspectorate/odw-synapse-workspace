resource "azurerm_log_analytics_workspace" "synapse" {
  name                = "pins-log-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.tags
}

resource "azurerm_log_analytics_storage_insights" "data_lake" {
  name                = "pins-log-si-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  workspace_id        = azurerm_log_analytics_workspace.synapse.id
  storage_account_id  = var.data_lake_account_id
  storage_account_key = data.azurerm_key_vault_secret.data_lake_storage_account_key.value
}
