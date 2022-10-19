locals {
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"
}
