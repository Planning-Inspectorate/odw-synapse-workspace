output "data_lake_account_id" {
  description = "The ID of the Data Lake Storage Account"
  value       = azurerm_storage_account.synapse.id
}

output "key_vault_id" {
  description = "The ID of the Key Vault"
  value       = azurerm_key_vault.synapse.id
}

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

output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = azurerm_synapse_workspace.synapse.name
}
