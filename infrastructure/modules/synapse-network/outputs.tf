output "devops_agent_subnet_name" {
  description = "The name of the subnet into which the devops agents will be deployed"
  value       = var.devops_agent_subnet_name
}

output "synapse_private_endpoint_subnet_name" {
  description = "The name of the subnet into which Synapse private endpoints should be deployed"
  value       = var.synapse_private_endpoint_subnet_name
}

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

output "vnet_subnet_prefixes" {
  description = "A map of subnet names to CIDR ranges deployed in this module"
  value       = { for k, v in azurerm_subnet.synapse : k => one(v.address_prefixes) }
}
