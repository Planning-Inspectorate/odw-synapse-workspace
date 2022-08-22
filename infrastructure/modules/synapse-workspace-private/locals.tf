locals {
  module_name     = "synapse-workspace-private"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  data_lake_role_assignments = flatten([
    for role, principals in var.data_lake_role_assignments : [
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
