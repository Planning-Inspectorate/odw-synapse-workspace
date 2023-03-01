resource "azurerm_resource_group" "odt_pe_backoffice_sb" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_pe_backoffice_sb_failover" {
  count = var.odt_back_office_service_bus_enabled && var.failover_deployment ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_pe_backoffice_sb_global" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_global}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_private_dns_zone" "back_office_private_dns_zone" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name                = "privatelink.servicebus.windows.net"
  resource_group_name = azurerm_resource_group.odt_pe_backoffice_sb_global[0].name

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "back_office_private_dns_zone_vnet_link" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name                  = "pins-pdns-vnet-link-backoffice-sb-${local.resource_suffix}"
  resource_group_name   = azurerm_resource_group.odt_pe_backoffice_sb_global[0].name
  private_dns_zone_name = azurerm_private_dns_zone.back_office_private_dns_zone[0].name
  virtual_network_id    = module.synapse_network.vnet_id
  registration_enabled  = false

  tags = local.tags
}

module "odt_pe_backoffice_sb" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  source = "./modules/odt-pe-backoffice-sb"

  environment                                     = var.environment
  resource_group_name                             = azurerm_resource_group.odt_pe_backoffice_sb[0].name
  location                                        = module.azure_region.location_cli
  service_name                                    = local.service_name
  odt_back_office_service_bus_name                = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_name_failover : var.odt_back_office_service_bus_name)
  odt_back_office_service_bus_resource_group_name = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_resource_group_name_failover : var.odt_back_office_service_bus_resource_group_name)
  odt_back_office_private_endpoint_dns_zone_id    = azurerm_private_dns_zone.back_office_private_dns_zone[0].id
  synapse_private_endpoint_subnet_name            = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets           = module.synapse_network.vnet_subnets

  tags = local.tags
  providers = {
    azurerm                = azurerm,
    azurerm.odt_backoffice = azurerm.odt_backoffice
  }
}

module "odt_pe_backoffice_sb_failover" {
  count = var.odt_back_office_service_bus_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/odt-pe-backoffice-sb"

  environment                                     = var.environment
  resource_group_name                             = azurerm_resource_group.odt_pe_backoffice_sb_failover[0].name
  location                                        = module.azure_region.location_cli
  service_name                                    = local.service_name
  odt_back_office_service_bus_name                = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_name_failover : var.odt_back_office_service_bus_name)
  odt_back_office_service_bus_resource_group_name = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_resource_group_name_failover : var.odt_back_office_service_bus_resource_group_name)
  odt_back_office_private_endpoint_dns_zone_id    = azurerm_private_dns_zone.back_office_private_dns_zone[0].id
  synapse_private_endpoint_subnet_name            = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets           = module.synapse_network_failover.vnet_subnets

  tags = local.tags

  providers = {
    azurerm                = azurerm,
    azurerm.odt_backoffice = azurerm.odt_backoffice
  }
}
