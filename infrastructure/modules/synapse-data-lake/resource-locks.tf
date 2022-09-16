resource "azurerm_management_lock" "data_lake" {
  name       = "Prevent Deletion"
  scope      = azurerm_storage_account.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"

  depends_on = [
    azurerm_private_endpoint.data_lake,
    azurerm_role_assignment.data_lake,
    azurerm_role_assignment.terraform,
    azurerm_storage_account.synapse
  ]
}

resource "azurerm_management_lock" "key_vault" {
  name       = "Prevent Deletion"
  scope      = azurerm_key_vault.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"

  depends_on = [
    azurerm_key_vault.synapse,
    azurerm_key_vault_secret.data_lake_storage_account_key,
    azurerm_role_assignment.key_vault,
    azurerm_role_assignment.key_vault_terraform
  ]
}
