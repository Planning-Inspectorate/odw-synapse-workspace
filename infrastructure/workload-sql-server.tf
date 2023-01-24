resource "azurerm_resource_group" "sql_server" {
  count = var.sql_server_enabled ? 1 : 0

  name     = "pins-rg-sqlserver-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "sql_server_failover" {
  count = var.sql_server_enabled && var.failover_deployment ? 1 : 0

  name     = "pins-rg-sqlserver-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "synapse_sql_server" {
  count = var.sql_server_enabled ? 1 : 0

  source = "./modules/synapse-sql-server"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.sql_server[0].name
  location            = module.azure_region.location_cli
  service_name        = local.service_name

  devops_agent_subnet_name          = module.synapse_network.devops_agent_subnet_name
  firewall_allowed_ip_addresses     = yamldecode(file(local.firewall_config_file_path))
  key_vault_id                      = module.synapse_data_lake.key_vault_id
  sql_server_aad_administrator      = var.synapse_aad_administrator
  sql_server_administrator_username = var.sql_server_administrator_username
  synapse_workspace_id              = module.synapse_workspace_private.synapse_workspace_id
  vnet_subnet_ids                   = module.synapse_network.vnet_subnets
  vnet_subnet_ids_failover          = module.synapse_network_failover.vnet_subnets

  depends_on = [
    module.synapse_data_lake,
    module.synapse_workspace_private
  ]

  tags = local.tags
}

module "synapse_sql_server_failover" {
  count = var.sql_server_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/synapse-sql-server"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.sql_server_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  devops_agent_subnet_name          = module.synapse_network_failover.devops_agent_subnet_name
  firewall_allowed_ip_addresses     = yamldecode(file(local.firewall_config_file_path))
  key_vault_id                      = module.synapse_data_lake_failover.key_vault_id
  sql_server_aad_administrator      = var.synapse_aad_administrator
  sql_server_administrator_username = var.sql_server_administrator_username
  synapse_workspace_id              = module.synapse_workspace_private_failover[0].synapse_workspace_id
  vnet_subnet_ids                   = module.synapse_network_failover.vnet_subnets
  vnet_subnet_ids_failover          = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_data_lake_failover,
    module.synapse_workspace_private_failover
  ]

  tags = local.tags
}
