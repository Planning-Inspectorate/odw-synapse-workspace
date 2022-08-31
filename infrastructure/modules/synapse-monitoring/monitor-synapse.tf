resource "azurerm_monitor_diagnostic_setting" "synapse" {
  name                       = "Synapse"
  target_resource_id         = var.synapse_workspace_id
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
    category = "BuiltinSqlReqsEnded"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "GatewayApiRequests"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "IntegrationActivityRuns"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "IntegrationPipelineRuns"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "IntegrationTriggerRuns"
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
    category = "SynapseLinkEvent"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  log {
    category = "SynapseRbacOperations"
    enabled  = true

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
