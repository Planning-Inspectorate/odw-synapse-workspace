output "data_lake_dfs_endpoint" {
  description = "The DFS endpoint URL of the Data Lake Storage Account"
  value       = module.synapse_workspace_private.data_lake_dfs_endpoint
}

output "data_resource_group_name" {
  description = "The name of the data application resource group"
  value       = azurerm_resource_group.data.name
}

output "key_vault_uri" {
  description = "The URI of the Key Vault"
  value       = module.synapse_workspace_private.key_vault_uri
}

output "service_bus_namespace_name" {
  description = "The name of the Service Bus Namespace"
  value       = module.synapse_ingestion.service_bus_namespace_name
}

output "synapse_dev_endpoint" {
  description = "The development connectivity endpoint for the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_endpoints["dev"]
}

output "synapse_dsql_endpoint" {
  description = "The dedicated SQL pool connectivity endpoint for the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_endpoints["sql"]
}

output "synapse_ssql_endpoint" {
  description = "The serverless SQL pool connectivity endpoint for the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_endpoints["sqlOnDemand"]
}

output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_workspace_name
}
