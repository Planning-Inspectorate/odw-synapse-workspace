resource "azurerm_role_assignment" "shir_vm" {
  scope                = azurerm_storage_account.shir.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_windows_virtual_machine.synapse.identity[0].principal_id
}

resource "azurerm_role_assignment" "terraform" {
  scope                = azurerm_storage_account.shir.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
