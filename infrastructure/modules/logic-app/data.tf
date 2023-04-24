data "azurerm_managed_api" "servicebus2" {
  name     = "servicebus-2"
  location = var.location
}

data "azurerm_managed_api" "zendesk" {
  name     = "zendesk"
  location = var.location
}
