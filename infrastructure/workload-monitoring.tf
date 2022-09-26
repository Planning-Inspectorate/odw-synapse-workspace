resource "azurerm_resource_group" "monitoring" {
  name     = "pins-rg-monitoring-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "monitoring_failover" {
  count = var.failover_deployment ? 1 : 0

  name     = "pins-rg-monitoring-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

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
  alert_scope_service_health               = var.alert_scope_service_health
  alert_threshold_data_lake_capacity_bytes = var.alert_threshold_data_lake_capacity_bytes
  data_lake_account_id                     = module.synapse_data_lake.data_lake_account_id
  key_vault_id                             = module.synapse_data_lake.key_vault_id
  service_bus_namespace_id                 = module.synapse_ingestion.service_bus_namespace_id
  spark_pool_enabled                       = var.spark_pool_enabled
  sql_pool_enabled                         = var.sql_pool_enabled
  synapse_spark_pool_id                    = module.synapse_workspace_private.synapse_spark_pool_id
  synapse_sql_pool_id                      = module.synapse_workspace_private.synapse_sql_pool_id
  synapse_workspace_id                     = module.synapse_workspace_private.synapse_workspace_id
  synapse_vnet_id                          = module.synapse_network.vnet_id

  depends_on = [
    module.synapse_data_lake,
    module.synapse_ingestion,
    module.synapse_network,
    module.synapse_workspace_private
  ]

  tags = local.tags
}

module "synapse_monitoring_failover" {
  count = var.failover_deployment ? 1 : 0

  source = "./modules/synapse-monitoring"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.monitoring_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  alert_group_platform_enabled             = var.alert_group_platform_enabled
  alert_group_platform_recipients          = var.alert_group_platform_recipients
  alert_group_synapse_enabled              = var.alert_group_synapse_enabled
  alert_group_synapse_recipients           = var.alert_group_synapse_recipients
  alert_scope_service_health               = var.alert_scope_service_health
  alert_threshold_data_lake_capacity_bytes = var.alert_threshold_data_lake_capacity_bytes
  data_lake_account_id                     = module.synapse_data_lake_failover.data_lake_account_id
  key_vault_id                             = module.synapse_data_lake_failover.key_vault_id
  service_bus_namespace_id                 = module.synapse_ingestion_failover.service_bus_namespace_id
  spark_pool_enabled                       = var.spark_pool_enabled
  sql_pool_enabled                         = var.sql_pool_enabled
  synapse_spark_pool_id                    = module.synapse_workspace_private_failover[0].synapse_spark_pool_id
  synapse_sql_pool_id                      = module.synapse_workspace_private_failover[0].synapse_sql_pool_id
  synapse_workspace_id                     = module.synapse_workspace_private_failover[0].synapse_workspace_id
  synapse_vnet_id                          = module.synapse_network_failover.vnet_id

  depends_on = [
    module.synapse_data_lake_failover,
    module.synapse_ingestion_failover,
    module.synapse_network_failover,
    module.synapse_workspace_private_failover
  ]

  tags = local.tags
}
