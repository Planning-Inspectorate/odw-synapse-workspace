resource "azurerm_role_assignment" "purview_storage" {
  count = var.link_purview ? 1 : 0

  scope                = var.data_lake_account_id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = var.purview_msi_id
}
