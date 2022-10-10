locals {
  module_name     = "synapse-network"
  nsg_path        = "providers/Microsoft.Network/networkSecurityGroups"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
