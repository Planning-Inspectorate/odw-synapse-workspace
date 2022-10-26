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
  data_lake_private_endpoint_dns_zone_id = azurerm_private_dns_zone.data_lake.id
  data_lake_lifecycle_rules              = jsondecode(file(local.lifecycle_policy_file_path))
  data_lake_replication_type             = var.data_lake_replication_type
  data_lake_retention_days               = var.data_lake_retention_days
  data_lake_role_assignments             = var.data_lake_role_assignments
  data_lake_storage_containers           = var.data_lake_storage_containers
  devops_agent_subnet_name               = module.synapse_network.devops_agent_subnet_name
  firewall_allowed_ip_addresses          = yamldecode(file(local.firewall_config_file_path))
  key_vault_private_endpoint_dns_zone_id = azurerm_private_dns_zone.key_vault.id
  key_vault_role_assignments             = var.key_vault_role_assignments
  network_resource_group_name            = azurerm_resource_group.network.name
  synapse_private_endpoint_subnet_name   = module.synapse_network.synapse_private_endpoint_subnet_name
  tenant_id                              = var.tenant_id
  vnet_subnet_ids                        = module.synapse_network.vnet_subnets
  vnet_subnet_ids_failover               = module.synapse_network_failover.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_network_failover
  ]
}

module "synapse_data_lake_failover" {
  source = "./modules/synapse-data-lake"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_failover.name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  data_lake_account_tier                 = var.data_lake_account_tier
  data_lake_private_endpoint_dns_zone_id = azurerm_private_dns_zone.data_lake.id
  data_lake_lifecycle_rules              = jsondecode(file(local.lifecycle_policy_file_path))
  data_lake_replication_type             = var.data_lake_replication_type
  data_lake_retention_days               = var.data_lake_retention_days
  data_lake_role_assignments             = var.data_lake_role_assignments
  data_lake_storage_containers           = var.data_lake_storage_containers
  devops_agent_subnet_name               = module.synapse_network_failover.devops_agent_subnet_name
  firewall_allowed_ip_addresses          = yamldecode(file(local.firewall_config_file_path))
  key_vault_private_endpoint_dns_zone_id = azurerm_private_dns_zone.key_vault.id
  key_vault_role_assignments             = var.key_vault_role_assignments
  network_resource_group_name            = azurerm_resource_group.network_failover.name
  synapse_private_endpoint_subnet_name   = module.synapse_network_failover.synapse_private_endpoint_subnet_name
  tenant_id                              = var.tenant_id
  vnet_subnet_ids                        = module.synapse_network_failover.vnet_subnets
  vnet_subnet_ids_failover               = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_network_failover
  ]
}
