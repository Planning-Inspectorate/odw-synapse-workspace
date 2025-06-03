# allow the synapse service principle to send on these topics
resource "azurerm_role_assignment" "topics_to_send" {
  for_each = { for i, val in local.topics_to_send_ids : i => val }

  scope                = each.value
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.synapse_workspace_principal_id
}

resource "azurerm_role_assignment" "topics_to_send_failover" {
  for_each = { for i, val in local.topics_to_send_ids_failover : i => val }

  scope                = each.value
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.synapse_workspace_failover_principal_id
}