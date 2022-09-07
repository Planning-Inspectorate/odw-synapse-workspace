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

# resource "azurerm_monitor_activity_log_alert" "synapse_workspace_deleted" {
#   name                = "Synapse Workspace Deleted"
#   resource_group_name = var.resource_group_name
#   scopes              = [var.synapse_workspace_id]
#   description         = "Triggers an alert if the Synapse Workspace is deleted"
#   enabled             = var.alert_group_platform_enabled

#   criteria {
#     resource_id    = var.synapse_workspace_id
#     operation_name = "Microsoft.Synapse/workspaces/delete"
#     caterogy       = "Administrative"
#   }

#   action {
#     action_group_id = azurerm_monitor_action_group.platform_alerts.id
#   }
# }
