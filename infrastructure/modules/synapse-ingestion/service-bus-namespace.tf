resource "azurerm_servicebus_namespace" "synapse" {
  name                = "sb-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Premium"
  capacity            = 1

  tags = local.tags
}
