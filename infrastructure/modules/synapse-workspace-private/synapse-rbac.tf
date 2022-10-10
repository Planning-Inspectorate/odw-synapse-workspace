resource "azurerm_synapse_role_assignment" "synapse" {
  for_each = {
    for assignment in local.synapse_role_assignments : "${assignment.role_definition_name}.${assignment.principal_id}" => assignment
  }

  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  role_name            = each.value.role_definition_name
  principal_id         = each.value.principal_id

  depends_on = [
    time_sleep.firewall_delay
  ]
}

resource "azurerm_synapse_workspace_aad_admin" "synapse" {
  login                = var.synapse_aad_administrator.username
  object_id            = var.synapse_aad_administrator.object_id
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  tenant_id            = var.tenant_id

  depends_on = [
    time_sleep.firewall_delay
  ]
}
