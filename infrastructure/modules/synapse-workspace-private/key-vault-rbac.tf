resource "azurerm_role_assignment" "synapse_msi_key_vault" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}
