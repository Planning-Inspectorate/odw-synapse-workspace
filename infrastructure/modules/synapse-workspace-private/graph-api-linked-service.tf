resource "azurerm_synapse_linked_service" "graph_api" {
  name                 = "ls_ms_graph_api"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  type                 = "RestService"

  # these properties are outlined here: 
  # https://learn.microsoft.com/en-us/azure/data-factory/connector-rest?tabs=data-factory#use-service-principal-authentication
  # the "AzureKeyVaultSecret" properties are outlined here:
  # https://learn.microsoft.com/en-us/azure/data-factory/store-credentials-in-key-vault#reference-secret-stored-in-key-vault
  type_properties_json = jsonencode({
    authenticationType                = "AadServicePrincipal",
    enableServerCertificateValidation = true,
    servicePrincipalId                = var.synapse_linked_service_graph.client_id,
    servicePrincipalKey = {
      type       = "AzureKeyVaultSecret"
      secretName = data.azurerm_key_vault_secret.graph_api_client_secret.name,
      store = {
        type          = "LinkedServiceReference"
        referenceName = "ls_kv"
      }
    },
    aadResourceId = "https://graph.microsoft.com",
    tenant        = var.tenant_id,
    url           = "https://graph.microsoft.com/v1.0"
  })

  integration_runtime {
    name = var.synapse_integration_runtime_name
  }
}

# to-do, manage the secret in terraform?
data "azurerm_key_vault_secret" "graph_api_client_secret" {
  name         = "synapse-graph-client-secret"
  key_vault_id = var.key_vault_id
}