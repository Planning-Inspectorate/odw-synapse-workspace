resource "azurerm_monitor_activity_log_alert" "synapse_workspace_deleted" {
  name                = "Synapse Workspace Deleted"
  resource_group_name = var.resource_group_name
  scopes              = [var.synapse_workspace_id]
  description         = "Triggers an alert if the Synapse Workspace is deleted"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category       = "Administrative"
    level          = "Critical"
    resource_id    = var.synapse_workspace_id
    operation_name = "Microsoft.Synapse/workspaces/delete"
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }

  tags = local.tags
}

resource "azurerm_monitor_activity_log_alert" "synapse_workspace_resource_health" {
  name                = "Synapse Workspace Resource Health"
  resource_group_name = var.resource_group_name
  scopes              = [var.synapse_workspace_id]
  description         = "Triggers an alert if the Synapse Workspace resource health changes"
  enabled             = var.alert_group_platform_enabled

  criteria {
    category    = "ResourceHealth"
    level       = "Informational"
    resource_id = var.synapse_workspace_id

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
