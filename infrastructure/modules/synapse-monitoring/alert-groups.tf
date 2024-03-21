resource "azurerm_monitor_action_group" "platform_alerts" {
  name                = "pins-ag-platform-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "ODW Platform"

  # we set emails in the action groups in Azure Portal - to avoid needing to manage
  # emails in terraform
  lifecycle {
    ignore_changes = [
      email_receiver
    ]
  }

  tags = local.tags
}

resource "azurerm_monitor_action_group" "synapse_alerts" {
  name                = "pins-ag-synapse-${local.resource_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "ODW Synapse"
  enabled             = var.alert_group_synapse_enabled

  # we set emails in the action groups in Azure Portal - to avoid needing to manage
  # emails in terraform
  lifecycle {
    ignore_changes = [
      email_receiver
    ]
  }

  tags = local.tags
}
