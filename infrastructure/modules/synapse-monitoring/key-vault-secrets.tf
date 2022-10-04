resource "azurerm_key_vault_secret" "application_insights_connection_string" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "application-insights-connection-string"
  value           = azurerm_application_insights.synapse.connection_string
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date
    ]
  }
}
