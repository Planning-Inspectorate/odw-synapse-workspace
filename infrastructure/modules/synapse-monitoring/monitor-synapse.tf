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

  enabled_log {
    category = "BuiltinSqlReqsEnded"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "GatewayApiRequests"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "IntegrationActivityRuns"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "IntegrationPipelineRuns"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "IntegrationTriggerRuns"

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
    category = "SynapseLinkEvent"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "SynapseRbacOperations"

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
