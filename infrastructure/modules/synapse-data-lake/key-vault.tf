resource "azurerm_key_vault" "synapse" {
  #checkov:skip=CKV_AZURE_109: TODO: Implement fine-grained Key Vault firewall rules
  name                       = replace("pins-kv-synw-${local.resource_suffix}", "-", "")
  resource_group_name        = var.resource_group_name
  location                   = var.location
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  tenant_id                  = data.azurerm_client_config.current.tenant_id

  tags = local.tags
}
