locals {
  module_name     = "odt-pe-backoffice-sb"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"
  resource_group  = var.odt_back_office_service_bus_failover_enabled == true ? var.odt_pe_backoffice_sb_failover_resource_group : var.odt_pe_backoffice_sb_resource_group

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}