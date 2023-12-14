locals {
  odt_ingestion_fn_app = {
    name    = "poc-odw-fa-odt-consumption"
    rg_name = "pins-rg-function-app-odw-dev-uks"
    enabled = var.environment == "dev"
  }
}

data "azurerm_linux_function_app" "odt_ingestion_function_app" {
  count               = local.odt_ingestion_fn_app.enabled ? 1 : 0
  name                = local.odt_ingestion_fn_app.name
  resource_group_name = local.odt_ingestion_fn_app.rg_name
}

resource "azurerm_role_assignment" "servicebus_data_receiver" {
  for_each = local.odt_ingestion_fn_app.enabled ? one(module.odt_backoffice_sb).subscription_ids : {}

  scope                = each.value
  role_definition_name = "Azure Service Bus Data Receiver"
  # TODO: Why is the output a list?
  principal_id = local.odt_ingestion_fn_app.enabled ? data.azurerm_linux_function_app.odt_ingestion_function_app[0].identity[0].principal_id : null
}

