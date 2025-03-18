resource "azurerm_synapse_workspace_aad_admin" "synapse" {
  login                = var.synapse_aad_administrator.username
  object_id            = var.synapse_aad_administrator.object_id
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  tenant_id            = var.tenant_id

  depends_on = [
    time_sleep.firewall_delay
  ]
}
