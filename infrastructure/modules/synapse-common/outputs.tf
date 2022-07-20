output "key_vault_id" {
  description = "The ID of the Key Vault to be used by Synapse and other resources"
  value       = azurerm_key_vault.synapse.id
}

output "synapse_private_dns_zone_id" {
  description = "The ID of the Private DNS Zone hosting privatelink.azuresynapse.net"
  value       = azurerm_private_dns_zone.synapse.id
}

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
