output "id" {
  description = "The ID of the Service Plan."
  value       = azurerm_service_plan.service_plan.id
}

output "name" {
  description = "The nsame of the SP"
  value       = azurerm_service_plan.service_plan.name
}
