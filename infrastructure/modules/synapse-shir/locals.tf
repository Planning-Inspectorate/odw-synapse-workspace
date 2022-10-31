locals {
  module_name     = "synapse-shir"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  script_name_deploy  = "Deploy-Requirements.ps1"
  script_name_openjdk = "Install-OpenJDK.ps1"
  script_name_runtime = "Initialize-IntegrationRuntime.ps1"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
