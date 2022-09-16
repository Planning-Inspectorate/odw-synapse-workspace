module "synapse_ingestion" {
  source = "./modules/synapse-ingestion"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.ingestion.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  tags = local.tags
}
