resource "azurerm_monitor_metric_alert" "data_lake_capacity" {
  name                = "Data Lake Capacity"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account exceeds 10TiB"
  enabled             = var.alert_group_platform_enabled
  frequency           = "PT15M"
  severity            = 2
  window_size         = "PT1H"

  criteria {
    metric_name      = "UsedCapacity"
    metric_namespace = "Microsoft.Storage/storageAccounts"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = var.alert_threshold_data_lake_capacity_bytes
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }

  tags = local.tags
}

resource "azurerm_monitor_metric_alert" "data_lake_latency" {
  name                = "Data Lake Latency"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account exceeds 1ms latency"
  enabled             = var.alert_group_platform_enabled
  frequency           = "PT1M"
  severity            = 2
  window_size         = "PT5M"

  criteria {
    metric_name      = "SuccessE2ELatency"
    metric_namespace = "Microsoft.Storage/storageAccounts"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 1000
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }
}
