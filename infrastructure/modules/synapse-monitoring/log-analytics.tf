resource "azurerm_log_analytics_workspace" "synapse" {
  name                = "log-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.tags
}
