resource "azurerm_servicebus_namespace_disaster_recovery_config" "failover" {
  count = var.failover_namespace ? 1 : 0

  name                 = "sb-${local.resource_suffix}"
  primary_namespace_id = var.primary_service_bus_namespace_id
  partner_namespace_id = azurerm_servicebus_namespace.synapse.id
}
