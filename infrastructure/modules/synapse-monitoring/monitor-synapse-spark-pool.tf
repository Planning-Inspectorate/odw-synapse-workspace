resource "azurerm_monitor_diagnostic_setting" "synapse_spark_pool" {
  count = var.spark_pool_enabled ? 1 : 0

  name                       = "SynapseSparkPool"
  target_resource_id         = var.synapse_spark_pool_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "Apache Spark Pool"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "BigDataPoolAppsEnded"

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
