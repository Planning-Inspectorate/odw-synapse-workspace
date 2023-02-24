resource "azurerm_key_vault" "management" {
  #checkov:skip=CKV_AZURE_189: Firewall is enabled in the network_acls block
  name                       = replace("pins-kv-mgmt-${local.resource_suffix}", "-", "")
  resource_group_name        = var.resource_group_name
  location                   = var.location
  sku_name                   = "standard"
  enabled_for_deployment     = true
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

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "List"
    ]
  }

  tags = local.tags
}
