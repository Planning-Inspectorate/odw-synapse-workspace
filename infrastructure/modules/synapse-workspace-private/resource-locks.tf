resource "azurerm_management_lock" "data_lake" {
  name       = "Prevent Deletion"
  scope      = azurerm_storage_account.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"
}

resource "azurerm_management_lock" "key_vault" {
  name       = "Prevent Deletion"
  scope      = azurerm_key_vault.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"
}

resource "azurerm_management_lock" "synapse" {
  name       = "Prevent Deletion"
  scope      = azurerm_synapse_workspace.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"
}
