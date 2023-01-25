resource "azurerm_monitor_diagnostic_setting" "network" {
  name                       = "Network"
  target_resource_id         = var.synapse_vnet_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "VMProtectionAlerts"

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
