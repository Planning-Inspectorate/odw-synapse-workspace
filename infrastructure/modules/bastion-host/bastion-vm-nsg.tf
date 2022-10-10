resource "azurerm_network_security_rule" "jumpbox_allow_host_comms_in" {
  name                        = "AllowBastionHostCommunicationInbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = var.synapse_vnet_security_groups[local.jumpbox_subnet_name]

  priority                   = 120
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "*"
  source_port_range          = "*"
  destination_port_ranges    = [22, 3389]
  source_address_prefix      = var.synapse_vnet_subnet_prefixes[local.bastion_subnet_name]
  destination_address_prefix = "VirtualNetwork"
}
