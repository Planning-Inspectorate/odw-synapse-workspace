resource "azurerm_resource_group" "network" {
  name     = "pins-rg-network-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "data_management" {
  name     = "pins-rg-datamgmt-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "data" {
  name     = "pins-rg-data-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}
