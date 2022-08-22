resource "azurerm_role_assignment" "data_lake" {
  for_each = toset(var.data_lake_rbac_blob_data_contributors)

  scope                = azurerm_storage_account.synapse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.key
}

resource "azurerm_role_assignment" "synapse_msi_data_lake" {
  scope                = azurerm_storage_account.synapse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse.identity.0.principal_id
}
