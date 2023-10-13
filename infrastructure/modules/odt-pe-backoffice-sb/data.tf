data "azurerm_resources" "odt_pe_backoffice_sb" {
  provider            = azurerm.odt
  resource_group_name = var.odt_back_office_service_bus_resource_group_name
  name                = var.odt_back_office_service_bus_name
  type                = "Microsoft.ServiceBus/namespaces"
}

data "azurerm_servicebus_topic" "odt_backoffice_topic" {
  provider            = azurerm.odt
  name                = "service-user"
  namespace_name      = var.odt_back_office_service_bus_name
  resource_group_name = var.odt_back_office_service_bus_resource_group_name
}
