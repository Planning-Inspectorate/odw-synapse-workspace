resource "azurerm_resource_group" "function_app" {
  count = var.function_app_enabled ? 1 : 0

  name     = "pins-rg-function-app-${local.resource_suffix}"
  location = module.azure_region.location_cli

  tags = local.tags
}

resource "azurerm_resource_group" "function_app_failover" {
  count = var.function_app_enabled && var.failover_deployment ? 1 : 0

  name     = "pins-rg-function-app-${local.resource_suffix_failover}"
  location = module.azure_region.paired_location.location_cli

  tags = local.tags
}

module "service_plan" {
  count = var.function_app_enabled ? 1 : 0

  source = "./modules/service-plan"

  resource_group_name = azurerm_resource_group.function_app[0].name
  service_name        = local.service_name
  environment         = var.environment
  location            = module.azure_region.location_cli
  tags                = local.tags
}

module "service_plan_failover" {
  count = var.function_app_enabled && var.failover_deployment ? 1 : 0

  source = "./modules/service-plan"

  resource_group_name = azurerm_resource_group.function_app_failover[0].name
  service_name        = local.service_name
  environment         = var.environment
  location            = module.azure_region.paired_location.location_cli
  tags                = local.tags
}

module "storage_account" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled == true
  }

  source = "./modules/storage-account"

  resource_group_name                     = azurerm_resource_group.function_app[0].name
  service_name                            = local.service_name
  environment                             = var.environment
  location                                = module.azure_region.location_cli
  tags                                    = local.tags
  network_rules_enabled                   = true
  network_rule_virtual_network_subnet_ids = concat([module.synapse_network.vnet_subnets[local.functionapp_subnet_name], module.synapse_network.vnet_subnets[local.compute_subnet_name]])
  shares = [
    {
      name  = "pins-${each.key}-${local.resource_suffix}"
      quota = 5120
    }
  ]
}

module "storage_account_failover" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled && var.failover_deployment == true
  }

  source = "./modules/storage-account"

  resource_group_name                     = azurerm_resource_group.function_app_failover[0].name
  service_name                            = local.service_name
  environment                             = var.environment
  location                                = module.azure_region.paired_location.location_cli
  tags                                    = local.tags
  network_rules_enabled                   = true
  network_rule_virtual_network_subnet_ids = concat([module.synapse_network_failover.vnet_subnets[local.functionapp_subnet_name], module.synapse_network_failover.vnet_subnets[local.compute_subnet_name]])
  shares = [
    {
      name  = "pins-${each.key}-${local.resource_suffix_failover}"
      quota = 5120
    }
  ]
}

module "function_app" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled == true
  }

  source = "./modules/function-app"

  resource_group_name          = azurerm_resource_group.function_app[0].name
  function_app_name            = each.key
  service_name                 = local.service_name
  service_plan_id              = module.service_plan[0].id
  storage_account_name         = module.storage_account[each.key].storage_name
  storage_account_access_key   = module.storage_account[each.key].primary_access_key
  environment                  = var.environment
  location                     = module.azure_region.location_cli
  tags                         = local.tags
  application_insights_key     = azurerm_application_insights.function_app_insights[each.key].instrumentation_key
  synapse_vnet_subnet_names    = module.synapse_network.vnet_subnets
  app_settings                 = try(each.value.app_settings, null)
  site_config                  = each.value.site_config
  file_share_name              = "pins-${each.key}-${local.resource_suffix}"
  servicebus_namespace         = var.odt_back_office_service_bus_name
  servicebus_namespace_appeals = var.odt_appeals_back_office.service_bus_name
  message_storage_account      = var.message_storage_account
  message_storage_container    = var.message_storage_container
}

module "function_app_failover" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled && var.failover_deployment == true
  }

  source = "./modules/function-app"

  resource_group_name        = azurerm_resource_group.function_app_failover[0].name
  function_app_name          = each.key
  service_name               = local.service_name
  service_plan_id            = module.service_plan_failover[0].id
  storage_account_name       = module.storage_account_failover[each.key].storage_name
  storage_account_access_key = module.storage_account_failover[each.key].primary_access_key
  environment                = var.environment
  location                   = module.azure_region.paired_location.location_cli
  tags                       = local.tags
  application_insights_key   = azurerm_application_insights.function_app_insights[each.key].instrumentation_key
  synapse_vnet_subnet_names  = module.synapse_network_failover.vnet_subnets
  app_settings               = try(each.value.app_settings, null)
  site_config                = each.value.site_config
  file_share_name            = "pins-${each.key}-${local.resource_suffix_failover}"
  servicebus_namespace       = var.odt_back_office_service_bus_name
}

resource "azurerm_role_assignment" "odt_servicebus_namespace" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled == true
  }

  scope                = module.odt_backoffice_sb[0].namespace_id
  role_definition_name = "Azure Service Bus Data receiver"
  principal_id         = module.function_app[each.key].identity[0].principal_id
}

resource "azurerm_role_assignment" "odt_appeals_servicebus_namespace" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled == true
  }

  scope                = module.odt_appeals_back_office_sb[0].namespace_id
  role_definition_name = "Azure Service Bus Data receiver"
  principal_id         = module.function_app[each.key].identity[0].principal_id
}

resource "azurerm_application_insights" "function_app_insights" {
  for_each = {
    for function_app in var.function_app : function_app.name => function_app if var.function_app_enabled == true
  }

  name                = "pins-${each.key}-${local.resource_suffix}-app-insights"
  location            = module.azure_region.location_cli
  resource_group_name = azurerm_resource_group.monitoring.name
  application_type    = "web"
  retention_in_days   = 30
  workspace_id        = module.synapse_monitoring.log_analytics_workspace_id

  tags = local.tags
}
