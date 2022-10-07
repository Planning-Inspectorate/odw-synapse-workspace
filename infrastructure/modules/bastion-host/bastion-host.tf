resource "azurerm_public_ip" "bastion_host" {
  name                = "bas-pip-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = local.tags
}

resource "azurerm_bastion_host" "bastion_host" {
  name                = "bas-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                 = "ipconfig"
    subnet_id            = var.synapse_vnet_subnet_names[local.bastion_subnet_name]
    public_ip_address_id = azurerm_public_ip.bastion_host.id
  }

  tags = local.tags
}
