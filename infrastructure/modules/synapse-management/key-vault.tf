resource "azurerm_key_vault" "management" {
  name                       = replace("pins-kv-mgmt-${local.resource_suffix}", "-", "")
  resource_group_name        = var.resource_group_name
  location                   = var.location
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  tenant_id                  = data.azurerm_client_config.current.tenant_id

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
