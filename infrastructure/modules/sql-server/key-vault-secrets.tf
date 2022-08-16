resource "azurerm_key_vault_secret" "sql_server_administrator_password" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "sql-server-administrator-password"
  value           = random_password.sql_server_administrator_password.result
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}

resource "azurerm_key_vault_secret" "sql_server_administrator_username" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "sql-server-administrator-username"
  value           = var.sql_server_administrator_username
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}
