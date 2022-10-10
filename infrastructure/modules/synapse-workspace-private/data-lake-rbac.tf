resource "azurerm_role_assignment" "synapse_msi_data_lake" {
  scope                = var.data_lake_account_id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}

resource "azurerm_role_assignment" "synapse_msi_data_lake_failover" {
  scope                = var.data_lake_account_id_failover
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}
