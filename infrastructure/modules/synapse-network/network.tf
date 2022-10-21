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
  for_each = module.subnets.network_cidr_blocks

  name                 = each.key
  resource_group_name  = var.resource_group_name
  address_prefixes     = [each.value]
  virtual_network_name = azurerm_virtual_network.synapse.name
  service_endpoints    = each.key == var.devops_agent_subnet_name ? local.devops_agent_subnet_service_endpoints : []
}
