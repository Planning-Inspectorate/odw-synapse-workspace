resource "azurerm_resource_group" "odt_backoffice_sb" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_backoffice_sb_failover" {
  count = var.odt_back_office_service_bus_enabled && var.failover_deployment ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "odt_backoffice_sb_global" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  name     = "pins-rg-odt-bo-sb-${local.resource_suffix_global}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_private_dns_zone" "back_office_private_dns_zone" {
  count = var.odt_back_office_service_bus_enabled && var.environment != "dev" ? 1 : 0

  name                = "privatelink.servicebus.windows.net"
  resource_group_name = azurerm_resource_group.odt_backoffice_sb_global[0].name

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "back_office_private_dns_zone_vnet_link" {
  count = var.odt_back_office_service_bus_enabled && var.environment != "dev" ? 1 : 0

  name                  = "pins-pdns-vnet-link-backoffice-sb-${local.resource_suffix}"
  resource_group_name   = azurerm_resource_group.odt_backoffice_sb_global[0].name
  private_dns_zone_name = azurerm_private_dns_zone.back_office_private_dns_zone[0].name
  virtual_network_id    = module.synapse_network.vnet_id
  registration_enabled  = false

  tags = local.tags
}

module "odt_backoffice_sb" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  environment                                     = var.environment
  resource_group_name                             = azurerm_resource_group.odt_backoffice_sb[0].name
  location                                        = module.azure_region.location_cli
  service_name                                    = local.service_name
  odt_backoffice_sb_topic_subscriptions           = var.odt_backoffice_sb_topic_subscriptions
  odt_back_office_service_bus_name                = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_name_failover : var.odt_back_office_service_bus_name)
  odt_back_office_service_bus_resource_group_name = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_resource_group_name_failover : var.odt_back_office_service_bus_resource_group_name)
  odt_back_office_private_endpoint_dns_zone_id    = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name            = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets           = module.synapse_network.vnet_subnets

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}

module "odt_backoffice_sb_failover" {
  count = var.odt_back_office_service_bus_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  environment                                     = var.environment
  resource_group_name                             = azurerm_resource_group.odt_backoffice_sb_failover[0].name
  location                                        = module.azure_region.location_cli
  service_name                                    = local.service_name
  odt_backoffice_sb_topic_subscriptions           = var.odt_backoffice_sb_topic_subscriptions
  odt_back_office_service_bus_name                = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_name_failover : var.odt_back_office_service_bus_name)
  odt_back_office_service_bus_resource_group_name = (var.odt_back_office_service_bus_failover_enabled == true ? var.odt_back_office_service_bus_resource_group_name_failover : var.odt_back_office_service_bus_resource_group_name)
  odt_back_office_private_endpoint_dns_zone_id    = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name            = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets           = module.synapse_network_failover.vnet_subnets

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}


module "odt_appeals_back_office_sb" {
  count = var.odt_appeals_back_office.service_bus_enabled ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  back_office_name                                = "appeals-backoffice"
  environment                                     = var.environment
  resource_group_name                             = azurerm_resource_group.odt_backoffice_sb[0].name
  location                                        = module.azure_region.location_cli
  service_name                                    = local.service_name
  odt_backoffice_sb_topic_subscriptions           = var.odt_appeals_back_office_sb_topic_subscriptions
  odt_back_office_service_bus_name                = var.odt_appeals_back_office.service_bus_name
  odt_back_office_service_bus_resource_group_name = var.odt_appeals_back_office.resource_group_name
  odt_back_office_private_endpoint_dns_zone_id    = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name            = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets           = module.synapse_network.vnet_subnets

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}
