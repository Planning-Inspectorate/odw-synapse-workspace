resource "azurerm_api_connection" "servicebus2" {
  name                = var.name
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.servicebus2.id
  display_name        = var.display_name

  parameter_values = {
    connectionString = "https://363f4178262a1d08.12.common.logic-uksouth.azure-apihub.net/apim/servicebus/32eeb0120347401eb61c2e2ba18188f8"
  }

  tags = local.tags

  lifecycle {
    # NOTE: since the connectionString is a secure value it's not returned from the API
    ignore_changes = [parameter_values]
  }
}

resource "azurerm_api_connection" "zendesk" {
  name                = var.name
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.zendesk.id
  display_name        = var.display_name

  parameter_values = {
    connectionString = "https://363f4178262a1d08.12.common.logic-uksouth.azure-apihub.net/apim/zendesk/27b0df658eb24f1fbc0e9287b57c6074"
  }

  tags = local.tags

  lifecycle {
    # NOTE: since the connectionString is a secure value it's not returned from the API
    ignore_changes = [parameter_values]
  }
}
