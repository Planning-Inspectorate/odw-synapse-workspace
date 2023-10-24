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
