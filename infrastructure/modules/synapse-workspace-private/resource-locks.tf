resource "azurerm_management_lock" "synapse" {
  name       = "Prevent Deletion"
  scope      = azurerm_synapse_workspace.synapse.id
  lock_level = "CanNotDelete"
  notes      = "Locked by Terraform"

  depends_on = [
    azurerm_private_endpoint.synapse_dedicated_sql_pool,
    azurerm_private_endpoint.synapse_development,
    azurerm_private_endpoint.synapse_serverless_sql_pool,
    azurerm_synapse_firewall_rule.allow_all,
    azurerm_synapse_firewall_rule.allow_all_azure,
    azurerm_synapse_managed_private_endpoint.data_lake,
    azurerm_synapse_role_assignment.synapse,
    azurerm_synapse_spark_pool.synapse,
    azurerm_synapse_sql_pool.synapse,
    azurerm_synapse_workspace.synapse,
    azurerm_synapse_workspace_aad_admin.synapse
  ]
}
