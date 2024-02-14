resource "azurerm_monitor_diagnostic_setting" "function_app" {
  for_each = var.function_app_ids

  name                       = "Function App Logs - ${each.key}"
  target_resource_id         = each.value
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id


  enabled_log {
    category = "FunctionAppLogs"
  }

  metric {
    category = "AllMetrics"
  }
}
