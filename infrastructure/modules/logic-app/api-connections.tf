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

resource "azurerm_resource_group_template_deployment" "zendesk_custom_api_template" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "zendesk-custom-api"
  resource_group_name = var.resource_group_name
  deployment_mode     = "Incremental"
  parameters_content = jsonencode({
    "connections" = {
      value = "zendesk-custom-api"
    }
    "customApiId" = {
      value = azapi_resource.zendesk_custom_api.id
    }
    "username" = {
      value = data.azurerm_key_vault_secret.zendesk_username.value
    }
    "password" = {
      value = data.azurerm_key_vault_secret.zendesk_password.value
    }
  })

  template_content = file("${path.module}/zendesk-template.json")

  lifecycle {
    ignore_changes = [
      parameters_content
    ]
  }
}
