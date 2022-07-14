output "purview_id" {
  description = "The ID of the Purview account to be used by Synapse and other resources"
  value       = azurerm_purview_account.management.id
}
