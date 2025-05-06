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

resource "azurerm_role_assignment" "purview_storage" {
  count = var.deploy_purview ? 1 : 0

  scope                = var.data_lake_account_id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_purview_account.management[0].identity[0].principal_id
}
