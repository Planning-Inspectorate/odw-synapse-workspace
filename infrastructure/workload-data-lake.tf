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

module "synapse_data_lake" {
  source = "./modules/synapse-data-lake"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  data_lake_account_tier                 = var.data_lake_account_tier
  data_lake_allowed_ip_addresses         = jsondecode(file(local.firewall_config_file_path))
  data_lake_private_endpoint_dns_zone_id = azurerm_private_dns_zone.data_lake.id
  data_lake_replication_type             = var.data_lake_replication_type
  data_lake_retention_days               = var.data_lake_retention_days
  data_lake_role_assignments             = var.data_lake_role_assignments
  data_lake_storage_containers           = var.data_lake_storage_containers
  key_vault_role_assignments             = var.key_vault_role_assignments
  network_resource_group_name            = azurerm_resource_group.network.name
  synapse_private_endpoint_subnet_name   = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets  = module.synapse_network.vnet_subnets
  tenant_id                              = var.tenant_id

  depends_on = [
    module.synapse_network
  ]
}

module "synapse_data_lake_failover" {
  source = "./modules/synapse-data-lake"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_failover.name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  data_lake_account_tier                 = var.data_lake_account_tier
  data_lake_allowed_ip_addresses         = jsondecode(file(local.firewall_config_file_path))
  data_lake_private_endpoint_dns_zone_id = azurerm_private_dns_zone.data_lake.id
  data_lake_replication_type             = var.data_lake_replication_type
  data_lake_retention_days               = var.data_lake_retention_days
  data_lake_role_assignments             = var.data_lake_role_assignments
  data_lake_storage_containers           = var.data_lake_storage_containers
  key_vault_role_assignments             = var.key_vault_role_assignments
  network_resource_group_name            = azurerm_resource_group.network_failover.name
  synapse_private_endpoint_subnet_name   = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets  = module.synapse_network_failover.vnet_subnets
  tenant_id                              = var.tenant_id

  depends_on = [
    module.synapse_network_failover
  ]
}
