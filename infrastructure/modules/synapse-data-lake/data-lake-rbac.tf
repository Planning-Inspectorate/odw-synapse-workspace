resource "azurerm_role_assignment" "data_lake" {
  for_each = {
    for assignment in local.data_lake_role_assignments : "${assignment.role_definition_name}.${assignment.principal_id}" => assignment
  }

  scope                = azurerm_storage_account.synapse.id
  role_definition_name = each.value.role_definition_name
  principal_id         = each.value.principal_id
}

resource "azurerm_role_assignment" "terraform" {
  scope                = azurerm_storage_account.synapse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
