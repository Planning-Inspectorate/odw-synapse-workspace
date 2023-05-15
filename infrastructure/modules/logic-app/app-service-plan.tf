resource "azurerm_service_plan" "logic_app" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "pins-app-service-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Windows"
  sku_name            = var.sku_name

  tags = local.tags
}
