resource "azurerm_monitor_action_group" "platform_alerts" {
  name                = "pins-ag-platform-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "ODW Alerts"
  enabled             = var.alert_group_platform_enabled

  dynamic "email_receiver" {
    for_each = var.alert_group_platform_recipients

    content {
      name                    = "Send to ${each.value}"
      email_address           = each.value
      use_common_alert_schema = true
    }
  }

  tags = local.tags
}

resource "azurerm_monitor_action_group" "synapse_alerts" {
  name                = "pins-ag-synapse-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "ODW Alerts"
  enabled             = var.alert_group_synapse_enabled

  dynamic "email_receiver" {
    for_each = var.alert_group_synapse_recipients

    content {
      name                    = "Send to ${each.value}"
      email_address           = each.value
      use_common_alert_schema = true
    }
  }

  tags = local.tags
}
