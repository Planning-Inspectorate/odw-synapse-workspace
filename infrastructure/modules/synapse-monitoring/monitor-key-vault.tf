resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  name                       = "KeyVault"
  target_resource_id         = var.key_vault_id
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
    category = "AuditEvent"

    retention_policy {
      days    = 0
      enabled = false
    }
  }

  enabled_log {
    category = "AzurePolicyEvaluationDetails"

    retention_policy {
      days    = 0
      enabled = false
    }
  }
}
