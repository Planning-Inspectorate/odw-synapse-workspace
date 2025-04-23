output "key_vault_id" {
  description = "The ID of the Key Vault use for management secrets"
  value       = azurerm_key_vault.management.id
}

output "purview_id" {
  description = "The ID of the Purview account to be used by Synapse and other resources"
  value       = var.deploy_purview ? azurerm_purview_account.management[0].id : null
}


output "purview_identity_principal_id" {
  description = "The ID of the Purview identity for RBAC"
  value       = var.deploy_purview ? azurerm_purview_account.management[0].identity[0].principal_id : null
}

output "purview_ids" {
  description = "The IDs of the Purview account and its managed resources"
  value = var.deploy_purview ? {
    id           = azurerm_purview_account.management[0].id,
    storage_id   = azurerm_purview_account.management[0].managed_resources[0].storage_account_id,
    event_hub_id = azurerm_purview_account.management[0].managed_resources[0].event_hub_namespace_id
  } : null
}