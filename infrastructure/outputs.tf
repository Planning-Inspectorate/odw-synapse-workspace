output "data_resource_group_name" {
  description = "The name of the data application resource group"
  value       = azurerm_resource_group.data.name
}

output "synapse_dsql_endpoint" {
  description = "The connectivity endpoint for the dedicated SQL pool"
  value       = module.synapse_workspace_private.synapse_endpoints["sql"]
}

output "synapse_ssql_endpoint" {
  description = "The connectivity endpoint for the serverless SQL pool"
  value       = module.synapse_workspace_private.synapse_endpoints["sqlOnDemand"]
}

output "synapse_workspace_name" {
  description = "The name of the Synapse Workspace"
  value       = module.synapse_workspace_private.synapse_workspace_name
}
