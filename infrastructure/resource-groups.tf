resource "azurerm_resource_group" "data" {
  name     = "pins-rg-data-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "data_failover" {
  name     = "pins-rg-data-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "data_management" {
  name     = "pins-rg-datamgmt-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "ingestion" {
  name     = "pins-rg-ingestion-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "ingestion_failover" {
  count = var.failover_deployment ? 1 : 0

  name     = "pins-rg-ingestion-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "monitoring" {
  name     = "pins-rg-monitoring-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "network" {
  name     = "pins-rg-network-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "network_failover" {
  name     = "pins-rg-network-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "network_global" {
  name     = "pins-rg-network-${local.resource_suffix_global}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "sql_server" {
  count = var.sql_server_enabled ? 1 : 0

  name     = "pins-rg-sqlserver-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}
