resource "azurerm_monitor_activity_log_alert" "key_vault_deleted" {
  name                = "Key Vault Deleted"
  resource_group_name = var.resource_group_name
  scopes              = [var.key_vault_id]
  description         = "Triggers an alert if the Key Vault is deleted"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category       = "Administrative"
    level          = "Critical"
    resource_id    = var.key_vault_id
    operation_name = "Microsoft.KeyVault/vaults/delete"
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }

  tags = local.tags
}

resource "azurerm_monitor_activity_log_alert" "key_vault_resource_health" {
  name                = "Key Vault Resource Health"
  resource_group_name = var.resource_group_name
  scopes              = [var.key_vault_id]
  description         = "Triggers an alert if the Key Vault resource health changes"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category    = "ResourceHealth"
    level       = "Informational"
    resource_id = var.key_vault_id

    resource_health {
      current = [
        "Available",
        "Degraded",
        "Unavailable",
        "Unknown"
      ]
      previous = [
        "Available",
        "Degraded",
        "Unavailable",
        "Unknown"
      ]
      reason = [
        "PlatformInitiated",
        "UserInitiated",
        "Unknown"
      ]
    }
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }

  tags = local.tags
}
