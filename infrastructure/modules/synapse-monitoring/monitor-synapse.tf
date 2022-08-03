resource "azurerm_monitor_diagnostic_setting" "synapse" {
  name                       = "Synapse"
  target_resource_id         = var.synapse_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.synapse.id

  log {
    category = "Integration Pipeline Runs"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }

  log {
    category = "Integration Activity Runs"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }

  log {
    category = "Integration Trigger Runs"
    enabled  = true

    retention_policy {
      enabled = false
    }
  }
}
