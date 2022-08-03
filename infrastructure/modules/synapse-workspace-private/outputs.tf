output "synapse_spark_pool_id" {
  description = "The ID of the Synapse Spark Pool"
  value       = azurerm_synapse_spark_pool.synapse[0].id
}

output "synapse_sql_pool_id" {
  description = "The ID of the Synapse SQL Pool"
  value       = azurerm_synapse_sql_pool.synapse[0].id
}

output "synapse_workspace_id" {
  description = "The ID of the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.id
}
