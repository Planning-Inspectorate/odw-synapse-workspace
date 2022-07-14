resource "azurerm_synapse_workspace" "synapse" {
  #checkov:skip=CKV2_AZURE_19:  TODO: Implement fine-grained Synapse firewall rules
  name                                 = "pins-synw-${local.resource_suffix}"
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  data_exfiltration_protection_enabled = true
  managed_resource_group_name          = "${var.resource_group_name}-synapse-managed"
  managed_virtual_network_enabled      = true
  purview_id                           = var.purview_id
  sql_administrator_login              = var.synapse_sql_administrator_username
  sql_administrator_login_password     = random_password.synapse_sql_administrator_password.result
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id

  dynamic "github_repo" {
    for_each = var.synapse_github_enabled ? [1] : []

    content {
      account_name    = var.synapse_github_details["account_name"]
      branch_name     = var.synapse_github_details["branch_name"]
      repository_name = var.synapse_github_details["repository_name"]
      root_folder     = var.synapse_github_details["root_folder"]
    }
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}
