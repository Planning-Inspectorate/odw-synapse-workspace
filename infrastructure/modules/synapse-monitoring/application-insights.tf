resource "azurerm_application_insights" "synapse" {
  name                = "pins-appi-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  application_type    = "other"
  retention_in_days   = var.log_retention_days
  workspace_id        = azurerm_log_analytics_workspace.synapse.id

  tags = local.tags
}

resource "azurerm_application_insights" "function_app_insights" {
  for_each = var.function_app_ids == null ? {} : var.function_app_ids

  name                = "${each.key}-app-insights"
  location            = var.location
  resource_group_name = var.resource_group_name
  application_type    = "web"
  retention_in_days   = var.log_retention_days
  workspace_id        = azurerm_log_analytics_workspace.synapse.id

  tags = local.tags
}
