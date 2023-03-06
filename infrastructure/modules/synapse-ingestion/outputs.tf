output "service_bus_namespace_id" {
  description = "The ID of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.id
}

output "service_bus_namespace_name" {
  description = "The name of the Service Bus Namespace deployed in this module"
  value       = azurerm_servicebus_namespace.synapse.name
}

output "topics" {
  description = "The list of topics deployed in this module"
  value       = local.service_bus_topics_and_subscriptions.*.topic_name
}

output "topic_subscriptions" {
  description = "The list of topic subscriptions deployed in this module"
  value       = local.service_bus_topics_and_subscriptions.*.subscription_name
}