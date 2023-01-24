resource "azurerm_resource_group" "ingestion" {
  name     = "pins-rg-ingestion-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "ingestion_failover" {
  name     = "pins-rg-ingestion-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "synapse_ingestion" {
  source = "./modules/synapse-ingestion"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.ingestion.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  failover_namespace                      = false
  service_bus_failover_enabled            = var.service_bus_failover_enabled
  service_bus_role_assignments            = var.service_bus_role_assignments
  synapse_workspace_failover_principal_id = try(module.synapse_workspace_private_failover.synapse_workspace_principal_id, null)
  synapse_workspace_principal_id          = module.synapse_workspace_private.synapse_workspace_principal_id

  tags = local.tags
}

module "synapse_ingestion_failover" {
  count = var.service_bus_failover_enabled ? 1 : 0

  source = "./modules/synapse-ingestion"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.ingestion_failover.name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  failover_namespace                      = true
  primary_service_bus_namespace_id        = module.synapse_ingestion.service_bus_namespace_id
  service_bus_failover_enabled            = var.service_bus_failover_enabled
  service_bus_role_assignments            = var.service_bus_role_assignments
  synapse_workspace_failover_principal_id = try(module.synapse_workspace_private_failover.synapse_workspace_principal_id, null)
  synapse_workspace_principal_id          = module.synapse_workspace_private.synapse_workspace_principal_id

  tags = local.tags
}
