output "data_lake_account_id" {
  description = "The ID of the Data Lake Storage Account"
  value       = azurerm_storage_account.synapse.id
}

output "data_lake_account_name" {
  description = "The name of the Data Lake Storage Account"
  value       = azurerm_storage_account.synapse.name
}

output "data_lake_dfs_endpoint" {
  description = "The DFS endpoint URL of the Data Lake Storage Account"
  value       = azurerm_storage_account.synapse.primary_dfs_endpoint
}

output "data_lake_filesystem_id" {
  description = "The ID of the Data Lake Gen2 filesystem"
  value       = azurerm_storage_data_lake_gen2_filesystem.synapse.id
}

output "key_vault_id" {
  description = "The ID of the Key Vault"
  value       = azurerm_key_vault.synapse.id
}

output "key_vault_uri" {
  description = "The URI of the Key Vault"
  value       = azurerm_key_vault.synapse.vault_uri
}
