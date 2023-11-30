resource "azurerm_monitor_diagnostic_setting" "synapse" {
  name                       = "Synapse"
  target_resource_id         = var.synapse_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    category = "AllMetrics"
    enabled  = true
  }

  enabled_log {
    category = "BuiltinSqlReqsEnded"
  }

  enabled_log {
    category = "GatewayApiRequests"
  }

  enabled_log {
    category = "IntegrationActivityRuns"
  }

  enabled_log {
    category = "IntegrationPipelineRuns"
  }

  enabled_log {
    category = "IntegrationTriggerRuns"
  }

  enabled_log {
    category = "SQLSecurityAuditEvents"
  }

  enabled_log {
    category = "SynapseLinkEvent"
  }

  enabled_log {
    category = "SynapseRbacOperations"
  }
}
