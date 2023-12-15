resource "azurerm_monitor_diagnostic_setting" "function_app" {
  name                       = "FunctionApp"
  target_resource_id         = var.function_app_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true
  }

  enabled_log {
    category = "FunctionAppLogs"
  }
}
