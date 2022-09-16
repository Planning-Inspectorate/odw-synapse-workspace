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
