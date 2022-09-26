output "data_lake_account_id" {
  description = "The ID of the Data Lake Storage Account"
  value       = var.failover_deployment ? module.synapse_data_lake_failover.data_lake_account_id : module.synapse_data_lake.data_lake_account_id
}

output "data_lake_account_id_failover" {
  description = "The ID of the Data Lake Storage Account"
  value       = var.failover_deployment ? module.synapse_data_lake.data_lake_account_id : module.synapse_data_lake_failover.data_lake_account_id
}

output "data_lake_dfs_endpoint" {
  description = "The DFS endpoint URL of the Data Lake Storage Account"
  value       = var.failover_deployment ? module.synapse_data_lake_failover.data_lake_dfs_endpoint : module.synapse_data_lake.data_lake_dfs_endpoint
}

output "data_resource_group_name" {
  description = "The name of the data application resource group"
  value       = var.failover_deployment ? azurerm_resource_group.data_failover.name : azurerm_resource_group.data.name
}

output "key_vault_uri" {
  description = "The URI of the Key Vault"
  value       = var.failover_deployment ? module.synapse_data_lake_failover.key_vault_uri : module.synapse_data_lake.key_vault_uri
}

output "service_bus_namespace_name" {
  description = "The name of the Service Bus Namespace"
  value       = var.failover_deployment ? module.synapse_ingestion_failover.service_bus_namespace_name : module.synapse_ingestion.service_bus_namespace_name
}

output "synapse_dev_endpoint" {
  description = "The development connectivity endpoint for the Synapse Workspace"
  value       = var.failover_deployment ? one(module.synapse_workspace_private_failover).synapse_endpoints["dev"] : module.synapse_workspace_private.synapse_endpoints["dev"]
}

output "synapse_dsql_endpoint" {
  description = "The dedicated SQL pool connectivity endpoint for the Synapse Workspace"
  value       = var.failover_deployment ? one(module.synapse_workspace_private_failover).synapse_endpoints["sql"] : module.synapse_workspace_private.synapse_endpoints["sql"]
}

output "synapse_ssql_endpoint" {
  description = "The serverless SQL pool connectivity endpoint for the Synapse Workspace"
  value       = var.failover_deployment ? one(module.synapse_workspace_private_failover).synapse_endpoints["sqlOnDemand"] : module.synapse_workspace_private.synapse_endpoints["sqlOnDemand"]
}

output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = var.failover_deployment ? one(module.synapse_workspace_private_failover).synapse_workspace_name : module.synapse_workspace_private.synapse_workspace_name
}
