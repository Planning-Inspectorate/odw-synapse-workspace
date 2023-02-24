locals {
  module_name     = "odt-pe-backoffice-sb"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"
  resource_group  = var.odt_back_office_service_bus_failover_enabled == true ? var.odt_pe_backoffice_service_bus_resource_group_name_failover : var.odt_pe_backoffice_service_bus_resource_group_name

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}