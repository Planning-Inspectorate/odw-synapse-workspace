resource "azurerm_key_vault" "synapse" {
  #checkov:skip=CKV_AZURE_189: Firewall is enabled in the network_acls block
  name                       = replace("pins-kv-synw-${local.resource_suffix}", "-", "")
  resource_group_name        = var.resource_group_name
  location                   = var.location
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  tenant_id                  = var.tenant_id

  network_acls {
    bypass         = "AzureServices"
    default_action = "Deny"
    ip_rules       = var.firewall_allowed_ip_addresses
    virtual_network_subnet_ids = [
      var.vnet_subnet_ids[var.devops_agent_subnet_name],
      var.vnet_subnet_ids_failover[var.devops_agent_subnet_name]
    ]
  }

  tags = local.tags
}
