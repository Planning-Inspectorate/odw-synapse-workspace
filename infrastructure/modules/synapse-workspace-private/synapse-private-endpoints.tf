resource "azurerm_synapse_private_link_hub" "synapse_workspace" {
  name                = replace("pins-pl-syn-ws-${local.resource_suffix}", "-", "")
  resource_group_name = var.network_resource_group_name
  location            = var.location
}

resource "azurerm_private_endpoint" "synapse_dedicated_sql_pool" {
  count = var.sql_pool_enabled ? 1 : 0

  name                = "pins-pe-syn-dsql-${local.resource_suffix}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "synapsePrivateDnsZone"
    private_dns_zone_ids = [var.synapse_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "synapseDedicatedSql"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_synapse_workspace.synapse.id
    subresource_names              = ["SQL"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "synapse_development" {
  name                = "pins-pe-syn-devops-${local.resource_suffix}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "synapsePrivateDnsZone"
    private_dns_zone_ids = [var.synapse_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "synapseDevelopment"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_synapse_workspace.synapse.id
    subresource_names              = ["DEV"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "synapse_serverless_sql_pool" {
  name                = "pins-pe-syn-ssql-${local.resource_suffix}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "synapsePrivateDnsZone"
    private_dns_zone_ids = [var.synapse_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "synapseServerlessSql"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_synapse_workspace.synapse.id
    subresource_names              = ["SqlOnDemand"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "synapse_workspace" {
  name                = "pins-pe-syn-ws-${local.resource_suffix}"
  resource_group_name = var.network_resource_group_name
  location            = var.location
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]

  private_dns_zone_group {
    name                 = "synapsePrivateDnsZone"
    private_dns_zone_ids = [var.synapse_private_endpoint_dns_zone_id]
  }

  private_service_connection {
    name                           = "synapseWorkspace"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_synapse_private_link_hub.synapse_workspace.id
    subresource_names              = ["Web"]
  }

  tags = local.tags
}

resource "azurerm_synapse_managed_private_endpoint" "data_lake" {
  name                 = "synapse-st-dfs--${azurerm_storage_account.synapse.name}"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  target_resource_id   = azurerm_storage_account.synapse.id
  subresource_name     = "dfs"

  depends_on = [
    time_sleep.firewall_delay
  ]
}
