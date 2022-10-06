resource "azurerm_network_security_group" "nsg" {
  for_each = { for k, v in azurerm_subnet.synapse : k => v.id }

  name                = "pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = local.tags
}

resource "azurerm_subnet_network_security_group_association" "nsg" {
  for_each = { for k, v in azurerm_subnet.synapse : k => v.id }

  network_security_group_id = "${var.resource_group_id}/${local.nsg_path}/pins-nsg-${lower(replace(each.key, "Subnet", ""))}-${local.resource_suffix}"
  subnet_id                 = each.value

  depends_on = [
    azurerm_network_security_group.nsg
  ]
}
