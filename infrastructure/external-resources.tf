data "azurerm_servicebus_namespace" "odt_appeals_backoffice_sb" {
  count               = var.odt_appeals_back_office.service_bus_enabled && var.external_resource_links_enabled ? 1 : 0
  name                = var.odt_appeals_back_office.service_bus_name
  resource_group_name = var.odt_appeals_back_office.resource_group_name

  provider = azurerm.odt
}

data "azurerm_servicebus_namespace" "odt_pe_backoffice_sb" {
  count               = var.external_resource_links_enabled ? 1 : 0
  name                = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_name_failover : var.odt_back_office_service_bus_name)
  resource_group_name = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_resource_group_name_failover : var.odt_back_office_service_bus_resource_group_name)

  provider = azurerm.odt
  depends_on = [
    module.synapse_ingestion
  ]
}
