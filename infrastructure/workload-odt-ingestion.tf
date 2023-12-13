locals {
  odt_ingestion_fn_app = {
    name = "odt-ingestion-fa"
  }
}

module "odt_ingestion_function_app" {
  count = var.odt_back_office_service_bus_enabled ? 1 : 0

  source = "./modules/function-app"

  resource_group_name        = azurerm_resource_group.function_app[0].name
  function_app_name          = local.odt_ingestion_fn_app.name
  service_name               = local.service_name
  service_plan_id            = module.service_plan[0].id
  storage_account_name       = module.storage_account[0].storage_name
  storage_account_access_key = module.storage_account[0].primary_access_key
  environment                = var.environment
  location                   = module.azure_region.location_cli
  tags                       = local.tags
  synapse_vnet_subnet_names  = module.synapse_network.vnet_subnets # TODO: This is likely not needed
  site_config                = var.function_app_site_config
  file_share_name            = "pins-${local.odt_ingestion_fn_app.name}-${local.resource_suffix}"
  servicebus_namespace       = var.odt_back_office_service_bus_name
}

resource "azurerm_role_assignment" "servicebus_data_receiver" {
  for_each = var.odt_back_office_service_bus_enabled ? one(module.odt_backoffice_sb).subscription_ids : []

  scope                = each.value
  role_definition_name = "Azure Service Bus Data Receiver"
  # TODO: Why is the output a list?
  principal_id = one(module.function_app).identity[0].principal_id
}

