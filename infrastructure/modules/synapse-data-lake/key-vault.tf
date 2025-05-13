resource "azurerm_key_vault" "synapse" {
  #checkov:skip=CKV2_AZURE_32: Managed Private Endpoint is enabled for the Key Vault
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
      var.vnet_subnet_ids_failover[var.devops_agent_subnet_name],
      data.azurerm_subnet.horizon_database.id
    ]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "key_vault" {
  name                = "pins-pe-${azurerm_key_vault.synapse.name}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "keyVaultDnsZone"
    private_dns_zone_ids = [var.key_vault_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "keyVault"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_key_vault.synapse.id
    subresource_names              = ["vault"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "tooling_key_vault" {
  name                = "pins-pe-${azurerm_key_vault.synapse.name}-tooling"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.vnet_subnet_ids[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "keyVaultPrivateDnsZone"
    private_dns_zone_ids = [var.tooling_config.key_vault_private_dns_zone_id]
  }

  private_service_connection {
    name                           = "keyVaultPrivateServiceConnection"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_key_vault.synapse.id
    subresource_names              = ["vault"]
  }

  tags = local.tags
}