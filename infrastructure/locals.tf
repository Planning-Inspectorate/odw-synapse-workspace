locals {
  location     = var.location
  service_name = "odw"

  firewall_config_file_path  = "${path.module}/configuration/firewall-rules/allowed_ip_addresses.yaml"
  lifecycle_policy_file_path = "${path.module}/configuration/data-lifecycle/policies.json"

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

  function_app_subscriptions = flatten([
    for function_app in var.function_app : [
      for subscriptions in [for v in one(module.odt_backoffice_sb).subscription_ids : v] : {
        name             = function_app.name
        subscription_ids = subscriptions
      }
    ]
  ])

  tags = merge(
    var.tags,
    {
      CreatedBy   = "Terraform"
      Environment = var.environment
      ServiceName = local.service_name
    }
  )
}
