resource "azurerm_servicebus_namespace_disaster_recovery_config" "failover" {
  count = var.failover_namespace && var.service_bus_failover_enabled ? 1 : 0

  name                 = "pins-sb-${local.resource_suffix}"
  primary_namespace_id = var.primary_service_bus_namespace_id
  partner_namespace_id = azurerm_servicebus_namespace.synapse.id
}
