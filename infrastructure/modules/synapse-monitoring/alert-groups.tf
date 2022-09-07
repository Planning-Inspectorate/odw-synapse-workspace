resource "azurerm_monitor_action_group" "platform_alerts" {
  name                = "pins-ag-platform-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "ODW Alerts"
  enabled             = var.alert_group_platform_enabled

  dynamic "email_receiver" {
    for_each = toset(var.alert_group_platform_recipients)
    iterator = mapping

    content {
      name                    = "Send to ${mapping.key}"
      email_address           = mapping.key
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
    for_each = toset(var.alert_group_synapse_recipients)
    iterator = mapping

    content {
      name                    = "Send to ${mapping.key}"
      email_address           = mapping.key
      use_common_alert_schema = true
    }
  }

  tags = local.tags
}
