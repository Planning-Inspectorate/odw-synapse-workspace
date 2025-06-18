resource "azurerm_synapse_sql_pool" "synapse" {
  #checkov:skip=CKV_AZURE_241: Ensure Synapse SQL pools are encrypted (checkov v3)
  #checkov:skip=CKV2_AZURE_51: Ensure Synapse SQL Pool has a security alert policy (checkov v3)
  #checkov:skip=CKV2_AZURE_54: Ensure log monitoring is enabled for Synapse SQL Pool (checkov v3)
  count = var.sql_pool_enabled ? 1 : 0

  name                 = "pinssyndpodw"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = var.sql_pool_sku_name
  collation            = var.sql_pool_collation
  create_mode          = "Default"
  storage_account_type = "GRS"

  tags = local.tags
}
