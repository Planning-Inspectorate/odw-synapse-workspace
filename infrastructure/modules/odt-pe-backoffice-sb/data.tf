data "azurerm_resources" "odt_pe_backoffice_sb" {
  provider            = azurerm.odt_backoffice
  resource_group_name = local.resource_group
  type                = "Microsoft.ServiceBus/namespaces"
}