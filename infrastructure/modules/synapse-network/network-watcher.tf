resource "azurerm_network_watcher" "synapse" {
  count = var.network_watcher_enabled ? 1 : 0

  name                = "nw-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = local.tags
}
