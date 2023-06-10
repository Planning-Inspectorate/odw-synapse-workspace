resource "azurerm_api_connection" "zendesk_api_connection" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "zendesk"
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.zendesk_managed_api.id
  display_name        = "pinssupport"

  parameter_values = {
    "token:Subdomain" = "pinssupport"
  }

  # lifecycle {
  #   ignore_changes = [
  #     parameter_values
  #   ]
  # }

  provisioner "local-exec" {
    command = "LogicAppConnectionAuth.ps1"
    interpreter = ["pwsh", "-Command"]
  }
}

resource "azurerm_api_connection" "service_bus_api_connection" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "servicebus"
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.service_bus_managed_api.id
  display_name        = "servicebus"

  parameter_values = {
    connectionString = var.service_bus_primary_connection_string
  }

  lifecycle {
    ignore_changes = [
      parameter_values
    ]
  }
}
