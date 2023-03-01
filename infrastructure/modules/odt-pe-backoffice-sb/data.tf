data "azurerm_resources" "odt_pe_backoffice_sb" {
  provider            = azurerm.odt_backoffice
  resource_group_name = local.resource_group
  name                = local.servicebus_name
  type                = "Microsoft.ServiceBus/namespaces"
}