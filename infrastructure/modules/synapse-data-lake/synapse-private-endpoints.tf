# resource "azurerm_synapse_managed_private_endpoint" "synapse_mpe_kv" {
#   name                 = "synapse-mpe-kv-${local.resource_suffix}"
#   synapse_workspace_id = var.synapse_workspace_id
#   target_resource_id   = azurerm_key_vault.synapse.id
#   subresource_name     = "vault"
# }

# resource "azurerm_synapse_managed_private_endpoint" "synapse_mpe_kv_failover" {
#   name                 = "synapse-failover-mpe-kv-${local.resource_suffix}"
#   synapse_workspace_id = var.synapse_workspace_id
#   target_resource_id   = azurerm_key_vault.synapse.id
#   subresource_name     = "vault"
# }
