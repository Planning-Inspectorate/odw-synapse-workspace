locals {
  location     = var.location
  service_name = "odw"

  resource_suffix = "${local.service_name}-${var.environment}-${module.azure_region.location_short}"

  compute_subnet_name = "ComputeSubnet"
  synapse_subnet_name = "SynapseEndpointSubnet"

  tags = merge(
    var.tags,
    {
      CreatedBy   = "Terraform"
      Environment = var.environment
      ServiceName = local.service_name
    }
  )
}
