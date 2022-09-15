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

module "synapse_network_failover" {
  source = "./modules/synapse-network"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.network_failover.name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  network_watcher_enabled = var.network_watcher_enabled
  vnet_base_cidr_block    = var.vnet_base_cidr_block_failover
  vnet_subnets            = var.vnet_subnets

  tags = local.tags
}

module "synapse_management" {
  source = "./modules/synapse-management"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  key_vault_role_assignments = var.key_vault_role_assignments

  tags = local.tags
}

module "synapse_ingestion" {
  source = "./modules/synapse-ingestion"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.ingestion.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  tags = local.tags
}

module "bastion_host" {
  count = var.bastion_host_enabled ? 1 : 0

  source = "./modules/bastion-host"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data_management.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  bastion_vm_image            = var.bastion_vm_image
  bastion_vm_username         = var.bastion_vm_username
  bastion_vm_size             = var.bastion_vm_size
  key_vault_id                = module.synapse_management.key_vault_id
  synapse_compute_subnet_name = local.compute_subnet_name
  synapse_vnet_subnets        = var.failover_deployment ? module.synapse_network_failover.vnet_subnets : module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_network_failover,
    module.synapse_management
  ]

  tags = local.tags
}

module "synapse_data_lake" {
  source = "./modules/synapse-data-lake"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  data_lake_account_tier                 = var.data_lake_account_tier
  data_lake_private_endpoint_dns_zone_id = var.failover_deployment ? module.synapse_network_failover.data_lake_private_dns_zone_id : module.synapse_network.data_lake_private_dns_zone_id
  data_lake_replication_type             = var.data_lake_replication_type
  data_lake_retention_days               = var.data_lake_retention_days
  data_lake_role_assignments             = var.data_lake_role_assignments
  data_lake_storage_containers           = var.data_lake_storage_containers
  network_resource_group_name            = azurerm_resource_group.network.name
  synapse_private_endpoint_subnet_name   = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets  = var.failover_deployment ? module.synapse_network_failover.vnet_subnets : module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network,
    module.synapse_network_failover
  ]
}

module "synapse_workspace_private" {
  source = "./modules/synapse-workspace-private"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.data.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  data_lake_account_id                  = module.synapse_data_lake.data_lake_account_id
  data_lake_account_name                = module.synapse_data_lake.data_lake_account_name
  data_lake_filesystem_id               = module.synapse_data_lake.data_lake_filesystem_id
  key_vault_role_assignments            = var.key_vault_role_assignments
  network_resource_group_name           = azurerm_resource_group.network.name
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
  synapse_data_exfiltration_enabled     = var.synapse_data_exfiltration_enabled
  synapse_private_endpoint_dns_zone_id  = var.failover_deployment ? module.synapse_network_failover.synapse_private_dns_zone_id : module.synapse_network.synapse_private_dns_zone_id
  synapse_private_endpoint_subnet_name  = local.synapse_subnet_name
  synapse_private_endpoint_vnet_subnets = var.failover_deployment ? module.synapse_network_failover.vnet_subnets : module.synapse_network.vnet_subnets
  synapse_sql_administrator_username    = var.synapse_sql_administrator_username
  synapse_role_assignments              = var.synapse_role_assignments

  depends_on = [
    module.synapse_data_lake,
    module.synapse_network,
    module.synapse_network_failover,
    module.synapse_management
  ]

  tags = local.tags
}

module "synapse_monitoring" {
  source = "./modules/synapse-monitoring"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.monitoring.name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  alert_group_platform_enabled             = var.alert_group_platform_enabled
  alert_group_platform_recipients          = var.alert_group_platform_recipients
  alert_group_synapse_enabled              = var.alert_group_synapse_enabled
  alert_group_synapse_recipients           = var.alert_group_synapse_recipients
  alert_threshold_data_lake_capacity_bytes = var.alert_threshold_data_lake_capacity_bytes
  data_lake_account_id                     = module.synapse_data_lake.data_lake_account_id
  key_vault_id                             = module.synapse_workspace_private.key_vault_id
  service_bus_namespace_id                 = module.synapse_ingestion.service_bus_namespace_id
  spark_pool_enabled                       = var.spark_pool_enabled
  sql_pool_enabled                         = var.sql_pool_enabled
  synapse_spark_pool_id                    = module.synapse_workspace_private.synapse_spark_pool_id
  synapse_sql_pool_id                      = module.synapse_workspace_private.synapse_sql_pool_id
  synapse_workspace_id                     = module.synapse_workspace_private.synapse_workspace_id
  synapse_vnet_id                          = var.failover_deployment ? module.synapse_network_failover.vnet_id : module.synapse_network.vnet_id

  depends_on = [
    module.synapse_data_lake,
    module.synapse_ingestion,
    module.synapse_network,
    module.synapse_network_failover,
    module.synapse_workspace_private
  ]

  tags = local.tags
}

module "synapse_sql_server" {
  count = var.sql_server_enabled ? 1 : 0

  source = "./modules/synapse-sql-server"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.sql_server[0].name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  key_vault_id                      = module.synapse_workspace_private.key_vault_id
  sql_server_aad_administrator      = var.synapse_aad_administrator
  sql_server_administrator_username = var.sql_server_administrator_username
  synapse_workspace_id              = module.synapse_workspace_private.synapse_workspace_id

  depends_on = [
    module.synapse_workspace_private
  ]

  tags = local.tags
}
