locals {
  module_name     = "odt-pe-backoffice-sb"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"
  
  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}

locals {
  resource_group = var.odt_pe_backoffice_sb_failover_enabled == true ? var.odt_pe_backoffice_sb_failover_resource_group : var.odt_pe_backoffice_sb_resource_group
} 