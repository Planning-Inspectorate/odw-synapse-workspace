module "subnets" {
  source  = "hashicorp/subnets/cidr"
  version = "1.0.0"

  base_cidr_block = var.vnet_base_cidr_block
  networks        = var.vnet_subnets
}

resource "azurerm_virtual_network" "synapse" {
  name                = "vnet-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  address_space       = [var.vnet_base_cidr_block]

  tags = local.tags
}

resource "azurerm_subnet" "synapse" {
  for_each = local.subnets

  name                 = each.key
  resource_group_name  = var.resource_group_name
  address_prefixes     = [each.value.cidr_block]
  virtual_network_name = azurerm_virtual_network.synapse.name
  service_endpoints    = each.value.service_endpoints

  dynamic "delegation" {
    for_each = each.value.service_delegation
    content {
      name = each.key
      service_delegation {
        name    = delegation.value.delegation_name
        actions = delegation.value.actions
      }
    }
  }
}
