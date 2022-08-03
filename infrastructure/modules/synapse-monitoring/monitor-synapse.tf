resource "azurerm_monitor_diagnostic_setting" "synapse" {
  name                       = "Synapse"
  target_resource_id         = var.synapse_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  dynamic "log" {
    for_each = var.synapse_diagnostic_settings

    content {
      category = each.value
      enabled  = true

      retention_policy {
        enabled = false
      }
    }
  }
}
