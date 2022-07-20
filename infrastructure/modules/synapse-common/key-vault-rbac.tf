resource "azurerm_role_assignment" "key_vault" {
  for_each = var.key_vault_role_assignments

  scope                = azurerm_key_vault.synapse.id
  role_definition_name = each.key
  principal_id         = each.value
}

resource "azurerm_role_assignment" "key_vault_terraform" {
  scope                = azurerm_key_vault.synapse.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}
