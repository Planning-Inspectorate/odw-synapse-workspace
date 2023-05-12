resource "azurerm_logic_app_workflow" "zendesk_created" {
  count               = var.logic_app_enabled ? 1 : 0
  name                = "zendesk-created"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = local.tags
}

resource "azurerm_logic_app_workflow" "zendesk_updated" {
  count               = var.logic_app_enabled ? 1 : 0
  name                = "zendesk-updated"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = local.tags
}
