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

resource "time_sleep" "firewall_delay" {
  create_duration = "30s"

  depends_on = [
    azurerm_synapse_firewall_rule.allow_all_azure,
    azurerm_synapse_firewall_rule.allow_all
  ]
}
