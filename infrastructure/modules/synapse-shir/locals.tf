locals {
  module_name     = "synapse-shir"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  script_name = "Initialize-IntegrationRuntime.ps1"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
