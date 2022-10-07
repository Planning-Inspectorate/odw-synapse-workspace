locals {
  bastion_subnet_name = "AzureBastionSubnet"
  jumpbox_subnet_name = "ComputeSubnet"
  module_name         = "bastion-host"
  resource_suffix     = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
