resource "azurerm_key_vault_secret" "synapse_sql_administrator_password" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "synapse-sql-administrator-password"
  value           = random_password.synapse_sql_administrator_password.result
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}

resource "azurerm_key_vault_secret" "synapse_sql_administrator_username" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "synapse-sql-administrator-username"
  value           = var.synapse_sql_administrator_username
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}
