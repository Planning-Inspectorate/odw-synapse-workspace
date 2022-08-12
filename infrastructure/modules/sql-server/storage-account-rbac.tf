resource "azurerm_role_assignment" "sql_server_auditing" {
  scope                = azurerm_storage_account.sql_server_auditing.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_mssql_server.sql_server.identity.0.principal_id
}
