resource "azurerm_synapse_linked_service" "graph_api" {
  name                 = "ls_ms_graph"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  type                 = "RestService"
  type_properties_json = <<JSON
{
    "authenticationType": "AadServicePrincipal",
    "clientId": "${var.synapse_linked_service_graph.client_id}",
    "clientSecret": "${local.graph_api_client_secret_reference}",
    "resource": "https://graph.microsoft.com",
    "tenant": "${var.tenant_id}",
    "url": "https://graph.microsoft.com/v1.0"
}
JSON

  integration_runtime {
    name = var.synapse_integration_runtime_name
  }
}

resource "azurerm_key_vault_secret" "graph_api_client_secret" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "synapse-graph-client-secret"
  value           = "to-replace"
  expiration_date = timeadd(timestamp(), "1h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}

locals {
  graph_api_client_secret_reference = "@Microsoft.KeyVault(https://${var.key_vault_name}.vault.azure.net/secrets/${azurerm_key_vault_secret.graph_api_client_secret.name}/)"
}