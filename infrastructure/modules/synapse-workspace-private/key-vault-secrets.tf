resource "azurerm_key_vault_secret" "data_lake_storage_account_key" {
  content_type    = "text/plain"
  key_vault_id    = azurerm_key_vault.synapse.id
  name            = "data-lake-storage-account-key"
  value           = data.azurerm_storage_account.synapse.primary_access_key
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }

  depends_on = [
    azurerm_role_assignment.key_vault_terraform
  ]
}

resource "azurerm_key_vault_secret" "synapse_sql_administrator_password" {
  content_type    = "text/plain"
  key_vault_id    = azurerm_key_vault.synapse.id
  name            = "synapse-sql-administrator-password"
  value           = random_password.synapse_sql_administrator_password.result
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }

  depends_on = [
    azurerm_role_assignment.key_vault_terraform
  ]
}

resource "azurerm_key_vault_secret" "synapse_sql_administrator_username" {
  content_type    = "text/plain"
  key_vault_id    = azurerm_key_vault.synapse.id
  name            = "synapse-sql-administrator-username"
  value           = var.synapse_sql_administrator_username
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }

  depends_on = [
    azurerm_role_assignment.key_vault_terraform
  ]
}
