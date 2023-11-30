output "resource_group_name" {
  description = "Name of the resource group where resources have been deployed to"
  value       = azurerm_linux_function_app.function[*].resource_group_name
}

output "name" {
  description = "Name of the function app"
  value       = azurerm_linux_function_app.function[*].name
}

output "function_app_id" {
  description = "Id of the function app"
  value       = azurerm_linux_function_app.function[*].id
}

output "hostname" {
  description = "Id of the function app"
  value       = azurerm_linux_function_app.function[*].default_hostname
}

output "identity" {
  description = "Identity block function app managed identity"
  value       = azurerm_linux_function_app.function[*].identity
}
