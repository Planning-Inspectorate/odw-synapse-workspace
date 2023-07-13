resource "azurerm_api_connection" "zendesk_api_connection" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "zendesk"
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.zendesk_managed_api.id
  display_name        = "pinssupport"

  parameter_values = {
    "token:Subdomain" = "pinssupport"
  }

  lifecycle {
    ignore_changes = [
      parameter_values
    ]
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

resource "azurerm_resource_group_template_deployment" "zendesk_custom_api" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "zendesk-custom-api"
  resource_group_name = var.resource_group_name
  deployment_mode     = "Incremental"
  parameters_content = jsonencode({
    "connections" = {
      value = "zendesk-custom-api"
    }
    "customApiId" = {
      value = data.azapi_resource.zendesk_custom_api.id
    }
  })

  template_content = file("${path.module}/zendesk-template.json")
}
