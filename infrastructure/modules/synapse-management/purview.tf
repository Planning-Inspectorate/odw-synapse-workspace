resource "azurerm_purview_account" "management" {
  count = var.deploy_purview ? 1 : 0

  name                        = "pins-pview-${local.resource_suffix}"
  resource_group_name         = var.resource_group_name
  location                    = var.location
  managed_resource_group_name = "${var.resource_group_name}-purview-managed"

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}
