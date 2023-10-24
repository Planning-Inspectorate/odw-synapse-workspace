data "azurerm_resources" "odt_pe_backoffice_sb" {
  provider            = azurerm.odt
  resource_group_name = var.odt_back_office_service_bus_resource_group_name
  name                = var.odt_back_office_service_bus_name
  type                = "Microsoft.ServiceBus/namespaces"
}

data "azurerm_servicebus_topic" "topic_id" {
  count               = length(local.odt_backoffice_sb_topic_names)
  provider            = azurerm.odt
  name                = local.odt_backoffice_sb_topic_names[count.index]
  namespace_name      = var.odt_back_office_service_bus_name
  resource_group_name = var.odt_back_office_service_bus_resource_group_name
}

data "azuread_group" "groups" {
  count        = length(local.group_names)
  display_name = local.group_names[count.index]
}

data "azuread_user" "users" {
  count               = length(local.user_principal_names)
  user_principal_name = local.user_principal_names[count.index]
}

data "azuread_service_principal" "service_principals" {
  count        = length(local.service_principal_names)
  display_name = local.service_principal_names[count.index]
}
