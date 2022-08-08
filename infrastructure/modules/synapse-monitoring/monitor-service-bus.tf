resource "azurerm_monitor_diagnostic_setting" "service_bus_namespace" {
  name                       = "ServiceBusNamespace"
  target_resource_id         = var.service_bus_namespace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "ApplicationMetricsLogs"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "OperationalLogs"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "RuntimeAuditLogs"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "VNetAndIPFilteringLogs"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
