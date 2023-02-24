resource "azurerm_resource_group" "odt_pe_backoffice_sb" {
  name     = "pins-rg-odt-bo-sb-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_pe_backoffice_sb_failover" {
  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_pe_backoffice_sb_global" {
  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_global}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_private_dns_zone" "back_office_private_dns_zone" {
  name                = "privatelink.servicebus.windows.net"
  resource_group_name = azurerm_resource_group.odt_pe_backoffice_sb_global.name

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "back_office_private_dns_zone_vnet_link" {
  name                  = "pins-pdns-vnet-link-backoffice-sb-${local.resource_suffix}"
  resource_group_name   = azurerm_resource_group.odt_pe_backoffice_sb_global.name
  private_dns_zone_name = azurerm_private_dns_zone.back_office_private_dns_zone.name
  virtual_network_id    = module.synapse_network.vnet_id
  registration_enabled  = false

  tags = local.tags
}

module "odt-pe-backoffice-sb" {
  source = "./modules/odt-pe-backoffice-sb"

  environment                                              = var.environment
  resource_group_name                                      = azurerm_resource_group.odt_pe_backoffice_sb.name
  location                                                 = azurerm_resource_group.odt_pe_backoffice_sb.location
  service_name                                             = local.service_name
  odt_back_office_service_bus_failover_enabled             = var.odt_back_office_service_bus_failover_enabled
  odt_back_office_service_bus_resource_group_name          = var.odt_back_office_service_bus_resource_group_name
  odt_back_office_service_bus_resource_group_name_failover = var.odt_back_office_service_bus_resource_group_name_failover
  odt_back_office_private_endpoint_dns_zone_id             = azurerm_private_dns_zone.back_office_private_dns_zone.id
  synapse_private_endpoint_subnet_name                     = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets                    = module.synapse_network.vnet_subnets

  tags = local.tags
}

module "odt-pe-backoffice-sb-failover" {
  count = var.failover_deployment ? 1 : 0

  source = "./modules/odt-pe-backoffice-sb"

  environment                                              = var.environment
  resource_group_name                                      = azurerm_resource_group.odt_pe_backoffice_sb_failover.name
  location                                                 = azurerm_resource_group.odt_pe_backoffice_sb_failover.location
  service_name                                             = local.service_name
  odt_back_office_service_bus_failover_enabled             = var.odt_back_office_service_bus_failover_enabled
  odt_back_office_service_bus_resource_group_name          = var.odt_back_office_service_bus_resource_group_name
  odt_back_office_service_bus_resource_group_name_failover = var.odt_back_office_service_bus_resource_group_name_failover
  odt_back_office_private_endpoint_dns_zone_id             = azurerm_private_dns_zone.back_office_private_dns_zone.id
  synapse_private_endpoint_subnet_name                     = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets                    = module.synapse_network_failover.vnet_subnets

  tags = local.tags
}
