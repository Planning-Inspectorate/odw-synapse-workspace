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

  tags = local.tags
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

  tags = local.tags
}

resource "azurerm_monitor_metric_alert" "data_lake_capacity" {
  name                = "Data Lake Capacity"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account exceeds the specified capacity"
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

resource "azurerm_monitor_metric_alert" "data_lake_throughput_egress" {
  name                = "Data Lake Egress"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account exceeds 100GB/s egress throughput"
  enabled             = var.alert_group_platform_enabled
  frequency           = "PT1M"
  severity            = 3
  window_size         = "PT1M"

  criteria {
    metric_name      = "Egress"
    metric_namespace = "Microsoft.Storage/storageAccounts"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 5998995570688 # 5.59TiB/minute (93.13GiB/s)
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }
}

resource "azurerm_monitor_metric_alert" "data_lake_throughput_ingress" {
  name                = "Data Lake Ingress"
  resource_group_name = var.resource_group_name
  scopes              = [var.data_lake_account_id]
  description         = "Triggers an alert if the Data Lake Gen2 Storage Account exceeds 50GB/s ingress throughput"
  enabled             = var.alert_group_platform_enabled
  frequency           = "PT1M"
  severity            = 3
  window_size         = "PT1M"

  criteria {
    metric_name      = "Ingress"
    metric_namespace = "Microsoft.Storage/storageAccounts"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 3000034656256 # 2.79TiB/minute (46.57GiB/s)
  }

  action {
    action_group_id = azurerm_monitor_action_group.platform_alerts.id
  }
}
