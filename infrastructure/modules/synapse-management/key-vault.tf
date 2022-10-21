resource "azurerm_key_vault" "management" {
  #checkov:skip=CKV_AZURE_109: TODO: Key vault firewall
  name                       = replace("pins-kv-mgmt-${local.resource_suffix}", "-", "")
  resource_group_name        = var.resource_group_name
  location                   = var.location
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  tenant_id                  = data.azurerm_client_config.current.tenant_id

  tags = local.tags
}
