locals {
  module_name     = "synapse-management"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  key_vault_role_assignments = flatten([
    for role, principals in var.key_vault_role_assignments : [
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
