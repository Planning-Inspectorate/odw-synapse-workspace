data "azurerm_resources" "odt_pe_backoffice_sb" {
  resource_group_name = local.resource_group 
  type                = "Microsoft.ServiceBus/namespaces"
  azure_provider      = azurerm.odt_backoffice
}