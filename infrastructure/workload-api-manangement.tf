resource "azurerm_resource_group" "api_management" {
  count = var.apim_enabled ? 1 : 0

  name     = "pins-rg-apim-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "api_management_failover" {
  count = var.apim_enabled && var.api_management_failover_enabled ? 1 : 0

  name     = "pins-rg-apim-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "api_management" {
  count = var.apim_enabled ? 1 : 0

  source = "./modules/api-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.api_management[0].name
  key_vault_id        = module.synapse_data_lake.key_vault_id
  location            = module.azure_region.location_cli
  publisher_name      = var.apim_publisher_name
  publisher_email     = var.apim_publisher_email
  service_name        = local.service_name
  sku_name            = var.apim_sku_name


  tags = local.tags
}

module "api_management_failover" {
  count = var.apim_enabled && var.api_management_failover_enabled ? 1 : 0

  source = "./modules/api-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.api_management_failover[0].name
  key_vault_id        = module.synapse_data_lake_failover.key_vault_id
  location            = module.azure_region.paired_location.location_cli
  publisher_name      = var.apim_publisher_name
  publisher_email     = var.apim_publisher_email
  service_name        = local.service_name
  sku_name            = var.apim_sku_name

  tags = local.tags
}
