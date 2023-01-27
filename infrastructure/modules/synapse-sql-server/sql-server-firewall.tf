resource "azurerm_mssql_firewall_rule" "allowed_ips" {
  for_each = toset(local.firewall_allowed_ip_addresses)

  name             = format("AllowRule%02s", index(local.firewall_allowed_ip_addresses, each.value) + 1)
  server_id        = azurerm_mssql_server.sql_server.id
  start_ip_address = cidrhost(each.value, 0)
  end_ip_address   = cidrhost(each.value, -1)
}
