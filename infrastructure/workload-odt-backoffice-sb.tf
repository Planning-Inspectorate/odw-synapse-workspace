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
  count = var.odt_back_office_service_bus_enabled && var.external_resource_links_enabled ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  environment                                  = var.environment
  resource_group_name                          = azurerm_resource_group.odt_backoffice_sb[0].name
  location                                     = module.azure_region.location_cli
  service_name                                 = local.service_name
  odt_backoffice_sb_topic_subscriptions        = var.odt_backoffice_sb_topic_subscriptions
  odt_back_office_service_bus_id               = local.odt_back_office_service_bus_id
  odt_back_office_private_endpoint_dns_zone_id = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name         = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets        = module.synapse_network.vnet_subnets
  synapse_workspace_failover_principal_id      = try(module.synapse_workspace_private_failover.synapse_workspace_principal_id, null)
  synapse_workspace_principal_id               = module.synapse_workspace_private.synapse_workspace_principal_id

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}

module "odt_backoffice_sb_failover" {
  count = var.odt_back_office_service_bus_enabled && var.failover_deployment && var.external_resource_links_enabled ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  environment                                  = var.environment
  resource_group_name                          = azurerm_resource_group.odt_backoffice_sb_failover[0].name
  location                                     = module.azure_region.location_cli
  service_name                                 = local.service_name
  odt_backoffice_sb_topic_subscriptions        = var.odt_backoffice_sb_topic_subscriptions
  odt_back_office_service_bus_id               = local.odt_back_office_service_bus_id
  odt_back_office_private_endpoint_dns_zone_id = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name         = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets        = module.synapse_network_failover.vnet_subnets
  synapse_workspace_failover_principal_id      = try(module.synapse_workspace_private_failover.synapse_workspace_principal_id, null)
  synapse_workspace_principal_id               = module.synapse_workspace_private.synapse_workspace_principal_id

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}


module "odt_appeals_back_office_sb" {
  count = var.odt_appeals_back_office.service_bus_enabled && var.external_resource_links_enabled ? 1 : 0

  source = "./modules/odt-backoffice-sb"

  back_office_name                             = "appeals-backoffice"
  environment                                  = var.environment
  resource_group_name                          = azurerm_resource_group.odt_backoffice_sb[0].name
  location                                     = module.azure_region.location_cli
  service_name                                 = local.service_name
  odt_backoffice_sb_topic_subscriptions        = var.odt_appeals_back_office_sb_topic_subscriptions
  odt_back_office_service_bus_id               = local.odt_appeals_back_office_service_bus_id
  odt_back_office_private_endpoint_dns_zone_id = (var.environment != "dev" ? azurerm_private_dns_zone.back_office_private_dns_zone[0].id : null)
  synapse_private_endpoint_subnet_name         = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets        = module.synapse_network.vnet_subnets
  synapse_workspace_failover_principal_id      = try(module.synapse_workspace_private_failover.synapse_workspace_principal_id, null)
  synapse_workspace_principal_id               = module.synapse_workspace_private.synapse_workspace_principal_id
  topics_to_send                               = ["listed-building"]

  tags = local.tags

  providers = {
    azurerm     = azurerm,
    azurerm.odt = azurerm.odt
  }
}


# For the dev environment
#moved {
#  from = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["/subscriptions/962e477c-0f3b-4372-97fc-a198a58e259e/resourceGroups/pins-rg-appeals-bo-dev/providers/Microsoft.ServiceBus/namespaces/pins-sb-appeals-bo-dev/topics/listed-building"]
#  to   = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["0"]
#}

# For the test environment
#moved {
#  from = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["/subscriptions/76cf28c6-6fda-42f1-bcd9-6d7dbed704ef/resourceGroups/pins-rg-appeals-bo-test/providers/Microsoft.ServiceBus/namespaces/pins-sb-appeals-bo-test/topics/listed-building"]
#  to   = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["0"]
#}

# For the prod environment
moved {
  from = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["/subscriptions/d1d6c393-2fe3-40af-ac27-f5b6bad36735/resourceGroups/pins-rg-appeals-bo-prod/providers/Microsoft.ServiceBus/namespaces/pins-sb-appeals-bo-prod/topics/listed-building"]
  to   = module.odt_appeals_back_office_sb[0].azurerm_role_assignment.topics_to_send["0"]
}
