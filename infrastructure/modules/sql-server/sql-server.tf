resource "azurerm_mssql_server" "sql_server" {
  #checkov:skip=CKV_AZURE_113:  TODO: Disable public network access
  #checkov:skip=CKV_AZURE_24:   TODO: Enable SQL Server auditing
  #checkov:skip=CKV_AZURE_23:   TODO: Enable SQL Server auditing
  name                = "sql-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  version             = "12.0"
  minimum_tls_version = "1.2"

  azuread_administrator {
    azuread_authentication_only = true
    login_username              = var.sql_server_aad_administrator["username"]
    object_id                   = var.sql_server_aad_administrator["object_id"]
    tenant_id                   = data.azurerm_client_config.current.tenant_id
  }

  tags = local.tags
}
