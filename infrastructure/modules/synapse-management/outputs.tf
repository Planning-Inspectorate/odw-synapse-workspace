output "key_vault_id" {
  description = "The ID of the Key Vault use for management secrets"
  value       = azurerm_key_vault.management.id
}
