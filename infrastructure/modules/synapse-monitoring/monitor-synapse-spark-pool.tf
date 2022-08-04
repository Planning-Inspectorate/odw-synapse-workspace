resource "azurerm_monitor_diagnostic_setting" "synapse_spark_pool" {
  count = can(var.synapse_spark_pool_id) ? 1 : 0

  name                       = "SynapseSparkPool"
  target_resource_id         = var.synapse_spark_pool_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }
}
