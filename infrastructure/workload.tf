module "synapse_network" {
  source = "./modules/synapse-network"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.network.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  network_watcher_enabled = var.network_watcher_enabled
  vnet_base_cidr_block    = var.vnet_base_cidr_block
  vnet_subnets            = var.vnet_subnets

  tags = local.tags
}

module "synapse_management" {
  source = "./modules/synapse-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  tags = local.tags
}

module "synapse_workspace_private" {
  source = "./modules/synapse-workspace-private"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  data_lake_account_tier                = var.data_lake_account_tier
  data_lake_replication_type            = var.data_lake_replication_type
  data_lake_role_assignments            = var.data_lake_role_assignments
  data_lake_storage_containers          = var.data_lake_storage_containers
  key_vault_role_assignments            = var.key_vault_role_assignments
  purview_id                            = module.synapse_management.purview_id
  spark_pool_enabled                    = var.spark_pool_enabled
  spark_pool_max_node_count             = var.spark_pool_max_node_count
  spark_pool_min_node_count             = var.spark_pool_min_node_count
  spark_pool_node_size                  = var.spark_pool_node_size
  spark_pool_version                    = var.spark_pool_version
  sql_pool_enabled                      = var.sql_pool_enabled
  sql_pool_collation                    = var.sql_pool_collation
  sql_pool_sku_name                     = var.sql_pool_sku_name
  synapse_aad_administrator             = var.synapse_aad_administrator
  synapse_private_endpoint_dns_zone_id  = module.synapse_network.synapse_private_dns_zone_id
  synapse_private_endpoint_subnet_name  = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets = module.synapse_network.vnet_subnets
  synapse_github_details                = var.synapse_github_details
  synapse_github_enabled                = var.synapse_github_enabled
  synapse_sql_administrator_username    = var.synapse_sql_administrator_username
  synapse_role_assignments              = var.synapse_role_assignments

  depends_on = [
    module.synapse_network,
    module.synapse_management
  ]

  tags = local.tags
}
