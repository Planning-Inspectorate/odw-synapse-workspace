output "synapse_endpoints" {
  description = "A list of connectivity endpoints associated with the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.connectivity_endpoints
}

output "synapse_spark_pool_id" {
  description = "The ID of the Synapse Spark Pool"
  value       = var.spark_pool_enabled ? one(azurerm_synapse_spark_pool.synapse34).id : null
}

output "synapse_sql_pool_id" {
  description = "The ID of the Synapse SQL Pool"
  value       = var.sql_pool_enabled ? one(azurerm_synapse_sql_pool.synapse).id : null
}

output "synapse_workspace_id" {
  description = "The ID of the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.id
}

output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.name
}

output "synapse_workspace_principal_id" {
  description = "The managed identity of the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.identity[0].principal_id
}
