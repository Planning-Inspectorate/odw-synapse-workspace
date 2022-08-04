resource "azurerm_monitor_diagnostic_setting" "network" {
  name                       = "Network"
  target_resource_id         = var.synapse_vnet_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  log {
    category = "VMProtectionAlerts"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = true
    }
  }
}
