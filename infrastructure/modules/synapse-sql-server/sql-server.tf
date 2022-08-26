resource "azurerm_mssql_server" "sql_server" {
  #checkov:skip=CKV_AZURE_113:  TODO: Disable public network access
  name                         = "sql-${local.resource_suffix}"
  location                     = var.location
  resource_group_name          = var.resource_group_name
  administrator_login          = var.sql_server_administrator_username
  administrator_login_password = random_password.sql_server_administrator_password.result
  version                      = "12.0"
  minimum_tls_version          = "1.2"

  azuread_administrator {
    azuread_authentication_only = false
    login_username              = var.sql_server_aad_administrator["username"]
    object_id                   = var.sql_server_aad_administrator["object_id"]
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}
