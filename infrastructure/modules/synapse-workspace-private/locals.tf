locals {
  module_name     = "synapse-workspace-private"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  firewall_allowed_ip_addresses = [
    for address in var.firewall_allowed_ip_addresses : can(split("/", address)[1]) ? address : "${address}/32"
  ]

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
