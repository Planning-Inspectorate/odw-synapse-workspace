output "vnet_id" {
  description = "The ID of the Virtual Network deployed in this module"
  value       = azurerm_virtual_network.synapse.id
}

output "vnet_name" {
  description = "The name of the Virtual Network deployed in this module"
  value       = azurerm_virtual_network.synapse.name
}

output "vnet_security_groups" {
  description = "A map of subnet names to network security group names deployed in this module"
  value       = { for k, v in azurerm_network_security_group.nsg : k => v.name }
}

output "vnet_subnets" {
  description = "A map of subnet names to IDs deployed in this module"
  value       = { for k, v in azurerm_subnet.synapse : k => v.id }
}
