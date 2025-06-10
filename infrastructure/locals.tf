locals {
  location     = var.location
  service_name = "odw"

  # no IP exceptions
  firewall_allowed_ip_addresses = []
  lifecycle_policy_file_path    = "${path.module}/configuration/data-lifecycle/policies.json"

  resource_suffix          = "${local.service_name}-${var.environment}-${module.azure_region.location_short}"
  resource_suffix_failover = "${local.service_name}-${var.environment}-${module.azure_region.paired_location.location_short}"
  resource_suffix_global   = "${local.service_name}-${var.environment}-global"

  apim_subnet_name        = "ApimSubnet"
  compute_subnet_name     = "ComputeSubnet"
  functionapp_subnet_name = "FunctionAppSubnet"
  synapse_subnet_name     = "SynapseEndpointSubnet"

  function_app_identity = {
    for function_app in module.function_app : function_app.name => function_app.identity[0].principal_id
  }

  function_app_ids = {
    for function_app in module.function_app : function_app.name => function_app.id
  }

  tags = merge(
    var.tags,
    {
      CreatedBy   = "Terraform"
      Environment = var.environment
      ServiceName = local.service_name
    }
  )

  tooling_storage_dns_zone_ids = {
    for zone in toset(local.storage_zones) : zone => data.azurerm_private_dns_zone.tooling_storage[zone].id
  }

  odt_back_office_service_bus_id         = var.odt_back_office_service_bus_failover_enabled == true ? "/subscriptions/${var.odt_subscription_id}/resourceGroups/${var.odt_back_office_service_bus_resource_group_name_failover}/providers/Microsoft.ServiceBus/namespaces/${var.odt_back_office_service_bus_name_failover}" : "/subscriptions/${var.odt_subscription_id}/resourceGroups/${var.odt_back_office_service_bus_resource_group_name}/providers/Microsoft.ServiceBus/namespaces/${var.odt_back_office_service_bus_name}"
  odt_appeals_back_office_service_bus_id = "/subscriptions/${var.odt_subscription_id}/resourceGroups/${var.odt_appeals_back_office.resource_group_name}/providers/Microsoft.ServiceBus/namespaces/${var.odt_appeals_back_office.service_bus_name}"
}
