resource "azurerm_synapse_integration_runtime_self_hosted" "synapse" {
  name                 = "pins-shir"
  synapse_workspace_id = var.synapse_workspace_id
}
