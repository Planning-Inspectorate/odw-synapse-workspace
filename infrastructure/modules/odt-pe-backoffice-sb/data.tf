data "azurerm_resources" "odt_pe_backoffice_sb" {
  provider            = azurerm.odt_backoffice
  resource_group_name = var.odt_back_office_service_bus_resource_group_name
  name                = var.odt_back_office_service_bus_name
  type                = "Microsoft.ServiceBus/namespaces"
}
