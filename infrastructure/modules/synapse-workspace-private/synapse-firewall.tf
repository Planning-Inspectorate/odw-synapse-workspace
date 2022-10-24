resource "azurerm_synapse_firewall_rule" "allow_all_azure" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

resource "azurerm_synapse_firewall_rule" "allow_all" {
  name                 = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}

resource "azurerm_synapse_firewall_rule" "allowed_ips" {
  for_each = local.firewall_allowed_ip_addresses

  name                 = "AllowRule${index(local.firewall_allowed_ip_addresses, each.value)}"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = cidrhost(each.value.address, 0)
  end_ip_address       = cidrhost(each.value.address, -1)
}

resource "time_sleep" "firewall_delay" {
  create_duration = "30s"

  depends_on = [
    azurerm_synapse_firewall_rule.allow_all_azure,
    azurerm_synapse_firewall_rule.allow_all
  ]
}
