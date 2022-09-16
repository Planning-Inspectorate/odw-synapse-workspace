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

module "synapse_sql_server_failover" {
  count = var.sql_server_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/synapse-sql-server"

  environment         = var.environment
  resource_group_name = azurerm_resource_group.sql_server_failover[0].name
  location            = module.azure_region.paired_location.location_cli
  service_name        = local.service_name

  key_vault_id                      = module.synapse_workspace_private_failover[0].key_vault_id
  sql_server_aad_administrator      = var.synapse_aad_administrator
  sql_server_administrator_username = var.sql_server_administrator_username
  synapse_workspace_id              = module.synapse_workspace_private_failover[0].synapse_workspace_id

  depends_on = [
    module.synapse_workspace_private_failover
  ]

  tags = local.tags
}
