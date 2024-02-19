resource "azurerm_monitor_metric_alert" "function_app_http_5xx" {
  for_each = var.function_app_ids == null ? {} : var.function_app_ids

  name                = "Http 5xx - ${each.key}"
  resource_group_name = var.resource_group_name
  enabled             = var.alert_group_platform_enabled
  scopes              = [each.value]
  description         = "Sends an alert when the Function App returns excess 5xx respones"
  window_size         = "PT5M"
  frequency           = "PT1M"
  severity            = 4

  criteria {
    metric_namespace = "Microsoft.Web/sites"
    metric_name      = "Http5xx"
    aggregation      = "Average"
    operator         = "GreaterThanOrEqual"
    threshold        = 10
  }

  action {
    action_group_id    = azurerm_monitor_action_group.platform_alerts.id
    webhook_properties = {}
  }

  tags = local.tags
}

resource "azurerm_monitor_metric_alert" "function_app_response_time" {
  for_each = var.function_app_ids == null ? {} : var.function_app_ids

  name                = "Response Time - ${each.key}"
  resource_group_name = var.resource_group_name
  enabled             = var.alert_group_platform_enabled
  scopes              = [each.value]
  description         = "Sends an alert when the Function App response exceeds 1 minute"
  window_size         = "PT5M"
  frequency           = "PT1M"
  severity            = 4

  criteria {
    metric_namespace = "Microsoft.Web/sites"
    metric_name      = "HttpResponseTime"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 60
  }

  action {
    action_group_id    = azurerm_monitor_action_group.platform_alerts.id
    webhook_properties = {}
  }

  tags = local.tags
}

resource "azurerm_monitor_activity_log_alert" "function_app_stop" {
  for_each = var.function_app_ids == null ? {} : var.function_app_ids

  name                = "Function App Stopped - ${each.key}"
  resource_group_name = var.resource_group_name
  enabled             = var.alert_group_platform_enabled
  scopes              = [each.value]
  description         = "Sends an alert when the Function App is stopped"

  criteria {
    resource_id    = each.value
    category       = "Administrative"
    operation_name = "Microsoft.Web/sites/stop/Action"
  }

  action {
    action_group_id    = azurerm_monitor_action_group.platform_alerts.id
    webhook_properties = {}
  }

  tags = local.tags
}

resource "azurerm_monitor_activity_log_alert" "function_app_delete" {
  for_each = var.function_app_ids == null ? {} : var.function_app_ids

  name                = "Function App Deleted - ${each.key}"
  resource_group_name = var.resource_group_name
  enabled             = var.alert_group_platform_enabled
  scopes              = [each.value]
  description         = "Sends an alert when the Function App is deleted"

  criteria {
    resource_id    = each.value
    category       = "Administrative"
    operation_name = "Microsoft.Web/sites/Delete"
  }

  action {
    action_group_id    = azurerm_monitor_action_group.platform_alerts.id
    webhook_properties = {}
  }

  tags = local.tags
}
