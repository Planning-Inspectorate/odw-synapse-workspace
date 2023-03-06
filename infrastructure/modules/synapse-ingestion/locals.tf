locals {
  module_name     = "synapse-ingestion"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  service_bus_topics_and_subscriptions = flatten([
    for topic, subscriptions in var.service_bus_topics_and_subscriptions : [
      for subscription in subscriptions : {
        topic_name        = topic
        subscription_name = subscription
      }
    ]
  ])

  service_bus_role_assignments = flatten([
    for role, principals in var.service_bus_role_assignments : [
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
