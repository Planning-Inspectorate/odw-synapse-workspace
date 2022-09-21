output "key_vault_id" {
  description = "The ID of the Key Vault use for management secrets"
  value       = azurerm_key_vault.management.id
}

output "purview_id" {
  description = "The ID of the Purview account to be used by Synapse and other resources"
  value       = var.deploy_purview ? azurerm_purview_account.management[0].id : null
}
