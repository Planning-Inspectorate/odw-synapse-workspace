resource "azurerm_monitor_diagnostic_setting" "synapse_sql_pool" {
  count = var.sql_pool_enabled ? 1 : 0

  name                       = "SynapseDedicatedSqlPool"
  target_resource_id         = var.synapse_sql_pool_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }

  enabled_log {
    category = "DmsWorkers"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "ExecRequests"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "RequestSteps"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "SQLSecurityAuditEvents"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "SqlRequests"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "Waits"

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
