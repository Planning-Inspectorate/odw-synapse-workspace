resource "azurerm_role_assignment" "key_vault" {
  for_each = {
    for assignment in local.key_vault_role_assignments : "${assignment.role_definition_name}.${assignment.principal_id}" => assignment
  }

  scope                = azurerm_key_vault.management.id
  role_definition_name = each.value.role_definition_name
  principal_id         = each.value.principal_id
}

resource "azurerm_role_assignment" "key_vault_terraform" {
  scope                = azurerm_key_vault.management.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "purview_msi_key_vault" {
  count = var.deploy_purview ? 1 : 0

  scope                = azurerm_key_vault.management.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_purview_account.management[0].identity[0].principal_id
}
