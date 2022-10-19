output "resource_group_name" {
  description = "The name of the resource group deployed in this module"
  value       = azurerm_resource_group.devops_agents.name
}
