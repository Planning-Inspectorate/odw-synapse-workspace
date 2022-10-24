locals {
  module_name     = "synapse-workspace-private"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  firewall_allowed_ip_addresses = [
    for address in var.firewall_allowed_ip_addresses : {
      address = can(split("/", address)[1]) ? address : "${address}/32"
    }
  ]

  synapse_role_assignments = flatten([
    for role, principals in var.synapse_role_assignments : [
      for principal in principals : {
        role_definition_name = role
        principal_id         = principal
      }
    ]
  ])

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
