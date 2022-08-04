resource "azurerm_monitor_diagnostic_setting" "synapse_sql_pool" {
  count = can(var.synapse_sql_pool_id) ? 1 : 0

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

  log {
    category = "DmsWorkers"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "ExecRequests"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "RequestSteps"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "SQLSecurityAuditEvents"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "SqlRequests"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "Waits"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
