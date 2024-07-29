output "integration_runtime_name" {
  description = "The name of the integration runtime"
  value       = azurerm_synapse_integration_runtime_self_hosted.synapse.name
}
