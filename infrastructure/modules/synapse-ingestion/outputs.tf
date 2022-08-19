output "service_bus_namespace_id" {
  description = "The ID of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.id
}

output "service_bus_namespace_name" {
  description = "The name of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.name
}
