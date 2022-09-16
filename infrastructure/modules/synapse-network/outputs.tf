output "vnet_id" {
  description = "The ID of the Virtual Network deployed in this module"
  value       = azurerm_virtual_network.synapse.id
}

output "vnet_name" {
  description = "The name of the Virtual Network deployed in this module"
  value       = azurerm_virtual_network.synapse.name
}

output "vnet_subnets" {
  description = "A map of subnet IDs and names deployed in this module"
  value       = { for k, v in azurerm_subnet.synapse : k => v.id }
}
