resource "azurerm_network_security_group" "bastion_host" {
  name                = "pins-nsg-bastion-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.network_resource_group_name

  tags = local.tags
}

resource "azurerm_network_security_rule" "bastion_allow_http_in" {
  #checkov:skip=CKV_AZURE_160:  Required for Azure Bastion
  #checkov:skip=CKV_AZURE_10:   Required for Azure Bastion
  #checkov:skip=CKV_AZURE_9:    Required for Azure Bastion
  name                        = "AllowHttpInbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 120
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "Tcp"
  source_port_range          = 443
  destination_port_range     = "*"
  source_address_prefix      = "Internet"
  destination_address_prefix = "*"
}

resource "azurerm_network_security_rule" "bastion_allow_gateway_manager_in" {
  name                        = "AllowGatewayManagerInbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 130
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "Tcp"
  source_port_range          = 443
  destination_port_range     = "*"
  source_address_prefix      = "GatewayManager"
  destination_address_prefix = "*"
}

resource "azurerm_network_security_rule" "bastion_allow_load_balancer_in" {
  name                        = "AllowAzureLoadBalancerInbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 140
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "Tcp"
  source_port_range          = 443
  destination_port_range     = "*"
  source_address_prefix      = "AzureLoadBalancer"
  destination_address_prefix = "*"
}

resource "azurerm_network_security_rule" "bastion_allow_host_comms_in" {
  name                        = "AllowBastionHostCommunicationInbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 150
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "*"
  source_port_ranges         = [5701, 8080]
  destination_port_range     = "*"
  source_address_prefix      = "VirtualNetwork"
  destination_address_prefix = "VirtualNetwork"
}

resource "azurerm_network_security_rule" "bastion_allow_ssh_rdp_out" {
  name                        = "AllowSshRdpOutbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 100
  direction                  = "Outbound"
  access                     = "Allow"
  protocol                   = "*"
  source_port_ranges         = [22, 3389]
  destination_port_range     = "*"
  source_address_prefix      = "*"
  destination_address_prefix = "VirtualNetwork"
}

resource "azurerm_network_security_rule" "bastion_allow_azure_cloud_out" {
  name                        = "AllowAzureCloudOutbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 110
  direction                  = "Outbound"
  access                     = "Allow"
  protocol                   = "Tcp"
  source_port_range          = 443
  destination_port_range     = "*"
  source_address_prefix      = "*"
  destination_address_prefix = "AzureCloud"
}

resource "azurerm_network_security_rule" "bastion_allow_host_comms_out" {
  name                        = "AllowBastionCommunicationOutbound"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 120
  direction                  = "Outbound"
  access                     = "Allow"
  protocol                   = "*"
  source_port_ranges         = [5701, 8080]
  destination_port_range     = "*"
  source_address_prefix      = "VirtualNetwork"
  destination_address_prefix = "VirtualNetwork"
}

resource "azurerm_network_security_rule" "bastion_allow_session_info_out" {
  name                        = "AllowGetSessionInformation"
  resource_group_name         = var.network_resource_group_name
  network_security_group_name = azurerm_network_security_group.bastion_host.name

  priority                   = 130
  direction                  = "Outbound"
  access                     = "Allow"
  protocol                   = "*"
  source_port_range          = 80
  destination_port_range     = "*"
  source_address_prefix      = "*"
  destination_address_prefix = "Internet"
}

resource "azurerm_subnet_network_security_group_association" "bastion_host" {
  network_security_group_id = azurerm_network_security_group.bastion_host
  subnet_id                 = var.synapse_vnet_subnets["AzureBastionSubnet"]
}
