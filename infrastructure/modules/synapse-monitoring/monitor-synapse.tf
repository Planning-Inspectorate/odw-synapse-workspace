resource "azurerm_monitor_diagnostic_setting" "synapse" {
  name                       = "Synapse"
  target_resource_id         = var.synapse_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  metric {
    # Total number of pipeline runs that occurred/ended within a 1-minute window
    category = "IntegrationPipelineRunsEnded"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }

  metric {
    # Total number of activity runs that occurred/ended within a 1-minute window
    category = "IntegrationActivityRunsEnded"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }

  metric {
    # Total number of trigger runs that occurred/ended within a 1-minute window
    category = "IntegrationTriggerRunsEnded"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }
}
