/*
    Terraform configuration file defining provider configuration
*/
locals {
  module_name     = "function-app"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )

  app_settings = merge(
    var.app_settings,
    {
      ServiceBusConnection__fullyQualifiedNamespace = "${var.servicebus_namespace}.servicebus.windows.net"
      "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING"    = "DefaultEndpointsProtocol=https;AccountName=${var.storage_account_name};AccountKey=${var.storage_account_access_key};EndpointSuffix=core.windows.net"
      "WEBSITE_CONTENTSHARE"                        = var.file_share_name
      "WEBSITE_CONTENTOVERVNET"                     = 1
      "WEBSITE_RUN_FROM_PACKAGE"                    = 1
      "ENABLE_ORYX_BUILD"                           = true
      "SCM_DO_BUILD_DURING_DEPLOYMENT"              = true
    }
  )

  site_config = merge(
    var.site_config_defaults,
    var.site_config
  )
}
