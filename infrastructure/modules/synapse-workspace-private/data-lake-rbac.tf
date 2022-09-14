resource "azurerm_role_assignment" "synapse_msi_data_lake" {
  scope                = data.azurerm_storage_account.synapse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse.identity.0.principal_id
}
