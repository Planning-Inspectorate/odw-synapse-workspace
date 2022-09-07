resource "azurerm_monitor_metric_alert" "synapse_pipeline_runs_failed" {
  name                = "Synapse Pipeline Runs Failed"
  resource_group_name = var.resource_group_name
  scopes              = [var.synapse_workspace_id]
  description         = "Triggers an alert if any Synapse pipeline runs fail"
  enabled             = var.alert_group_synapse_enabled

  criteria {
    metric_name      = "IntegrationPipelineRunsEnded"
    metric_namespace = "Microsoft.Synapse/workspaces"
    aggregation      = "Count"
    operator         = "GreaterThanOrEqual"
    threshold        = 1

    dimension {
      name     = "Result"
      operator = "Include"
      values   = ["Failed"]
    }
  }

  action {
    action_group_id = azurerm_monitor_action_group.synapse_alerts.id
  }
}
