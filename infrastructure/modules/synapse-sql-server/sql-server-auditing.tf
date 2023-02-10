resource "azurerm_mssql_server_extended_auditing_policy" "sql_server_auditing" {
  server_id         = azurerm_mssql_server.sql_server.id
  storage_endpoint  = azurerm_storage_account.sql_server_auditing.primary_blob_endpoint
  retention_in_days = 120

  depends_on = [
    azurerm_role_assignment.sql_server_auditing,
    azurerm_storage_account.sql_server_auditing
  ]
}
