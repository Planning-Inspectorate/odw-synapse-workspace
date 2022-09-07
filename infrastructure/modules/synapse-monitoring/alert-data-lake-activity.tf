resource "azurerm_monitor_activity_log_alert" "data_lake_deleted" {
  name                = "Data Lake Deleted"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account is deleted"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category       = "Administrative"
    level          = "Critical"
    operation_name = "Microsoft.Storage/storageAccounts/delete"
    resource_id    = var.data_lake_account_id
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }
}

resource "azurerm_monitor_activity_log_alert" "data_lake_resource_health" {
  name                = "Data Lake Resource Health"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account resource health changes"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category    = "ResourceHealth"
    level       = "Informational"
    resource_id = var.data_lake_account_id

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
}
