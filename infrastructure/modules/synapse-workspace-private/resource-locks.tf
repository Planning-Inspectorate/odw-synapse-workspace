resource "azurerm_management_lock" "data_lake" {
  name       = "Prevent Deletion"
  scope      = azurerm_storage_account.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"

  depends_on = [
    azurerm_private_endpoint.data_lake,
    azurerm_role_assignment.data_lake,
    azurerm_role_assignment.terraform,
    azurerm_role_assignment.synapse_msi_data_lake,
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
    azurerm_key_vault_secret.synapse_sql_administrator_password,
    azurerm_key_vault_secret.synapse_sql_administrator_username,
    azurerm_role_assignment.key_vault,
    azurerm_role_assignment.key_vault_terraform,
    azurerm_role_assignment.synapse_msi_key_vault
  ]
}

resource "azurerm_management_lock" "synapse" {
  name       = "Prevent Deletion"
  scope      = azurerm_synapse_workspace.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"

  depends_on = [
    azurerm_private_endpoint.synapse_dedicated_sql_pool,
    azurerm_private_endpoint.synapse_development,
    azurerm_private_endpoint.synapse_serverless_sql_pool,
    azurerm_synapse_firewall_rule.allow_all,
    azurerm_synapse_firewall_rule.allow_all_azure,
    azurerm_synapse_managed_private_endpoint.data_lake,
    azurerm_synapse_role_assignment.synapse,
    azurerm_synapse_spark_pool.synapse,
    azurerm_synapse_sql_pool.synapse,
    azurerm_synapse_workspace.synapse,
    azurerm_synapse_workspace_aad_admin.synapse
  ]
}
