data "azurerm_managed_api" "zendesk_managed_api" {
  location = var.location
  name     = "zendesk"
}

data "azurerm_managed_api" "service_bus_managed_api" {
  location = var.location
  name     = "servicebus"
}
