resource "azurerm_service_plan" "service_plan" {
  name                         = "pins-sp-${local.resource_suffix}"
  location                     = var.location
  resource_group_name          = var.resource_group_name
  os_type                      = var.os_type
  maximum_elastic_worker_count = var.maximum_elastic_worker_count
  sku_name                     = var.sku_name
  app_service_environment_id   = var.app_service_env_id
  per_site_scaling_enabled     = var.per_site_scaling_enabled
  tags                         = local.tags
  worker_count                 = var.worker_count
  zone_balancing_enabled       = var.zone_balancing_enabled
}
