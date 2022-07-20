resource "azurerm_role_assignment" "data_lake" {
  for_each = var.data_lake_role_assignments

  scope                = azurerm_storage_account.synapse.id
  role_definition_name = each.key
  principal_id         = each.value
}

resource "azurerm_role_assignment" "synapse_msi_data_lake" {
  scope                = azurerm_storage_account.synapse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse.identity.0.principal_id
}
