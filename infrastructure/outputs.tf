output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_workspace_name
}

output "data_resource_group_name" {
  description = "The name of the data application resource group"
  value       = azurerm_resource_group.data.name
}
