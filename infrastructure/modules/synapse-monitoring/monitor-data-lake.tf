resource "azurerm_monitor_diagnostic_setting" "data_lake" {
  name                       = "DataLake"
  target_resource_id         = var.data_lake_account_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "Capacity"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = true
    }
  }

  metric {
    category = "Transaction"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = true
    }
  }
}
