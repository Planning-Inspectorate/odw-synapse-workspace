resource "azurerm_role_assignment" "service_bus" {
  for_each = {
    for assignment in local.service_bus_role_assignments : "${assignment.role_definition_name}.${assignment.principal_id}" => assignment
  }

  scope                = azurerm_servicebus_namespace.synapse.id
  role_definition_name = each.value.role_definition_name
  principal_id         = each.value.principal_id
}

resource "azurerm_role_assignment" "synapse_pri_service_bus_rx" {
  scope                = azurerm_servicebus_namespace.synapse.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.synapse_workspace_principal_id
}

resource "azurerm_role_assignment" "synapse_sec_service_bus_rx" {
  count = var.synapse_workspace_failover_principal_id == null ? 0 : 1

  scope                = azurerm_servicebus_namespace.synapse.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.synapse_workspace_failover_principal_id
}

resource "azurerm_role_assignment" "synapse_pri_service_bus_tx" {
  scope                = azurerm_servicebus_namespace.synapse.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.synapse_workspace_principal_id
}

resource "azurerm_role_assignment" "synapse_sec_service_bus_tx" {
  count = var.synapse_workspace_failover_principal_id == null ? 0 : 1

  scope                = azurerm_servicebus_namespace.synapse.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.synapse_workspace_failover_principal_id
}
