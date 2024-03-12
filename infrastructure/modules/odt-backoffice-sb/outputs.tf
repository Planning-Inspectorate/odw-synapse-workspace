output "name" {
  description = "The name of the private endpoint."
  value       = azurerm_private_endpoint.odt_backoffice_servicebus_private_endpoint[*].name
}

output "id" {
  description = "The ID of the private endpoint."
  value       = azurerm_private_endpoint.odt_backoffice_servicebus_private_endpoint[*].id
}

output "location" {
  description = "value of the location."
  value       = azurerm_private_endpoint.odt_backoffice_servicebus_private_endpoint[*].location
}

output "resource_group_name" {
  description = "value of the resource group name."
  value       = azurerm_private_endpoint.odt_backoffice_servicebus_private_endpoint[*].resource_group_name
}

output "subscription_ids" {
  description = "A map of Subscription Name to Subscription Keys (used for consumer RBAC assignments)"
  value = {
    for key, subscription in azurerm_servicebus_subscription.odt_backoffice_subscriptions :
    subscription.name => subscription.id
  }
}

output "namespace_id" {
  description = "The ID of the Service Bus Namespace."
  value       = data.azurerm_resources.odt_pe_backoffice_sb.resources[0].id
}