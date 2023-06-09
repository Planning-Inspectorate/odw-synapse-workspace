output "service_bus_namespace_id" {
  description = "The ID of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.id
}

output "service_bus_namespace_name" {
  description = "The name of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.name
}

output "service_bus_primary_connection_string" {
  description = "The name of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.default_primary_connection_string
  sensitive   = true
}
