locals {
  module_name     = "synapse-ingestion"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  service_bus_topics_defaults = {
    status                                  = "Active"
    auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
    default_message_ttl                     = "P14D"
    duplicate_detection_history_time_window = "PT10M"
    enable_batched_operations               = false
    enable_express                          = false
    enable_partitioning                     = false
    max_size_in_megabytes                   = 5120
    requires_duplicate_detection            = false
    support_ordering                        = false
    subscriptions                           = {}
  }

  service_bus_topics_and_subscriptions = { for v in var.service_bus_topics_and_subscriptions : v.name => merge(local.service_bus_topics_defaults, v) }

  service_bus_topic_subscriptions_defaults = {
    status                                    = "Active"
    max_delivery_count                        = 1
    auto_delete_on_idle                       = "P10675199DT2H48M5.4775807S"
    default_message_ttl                       = "P14D"
    lock_duration                             = "PT1M"
    dead_lettering_on_message_expiration      = false
    dead_lettering_on_filter_evaluation_error = true
    enable_batched_operations                 = false
    requires_session                          = false
    forward_to                                = ""
    role_assignments                          = {}
  }

  service_bus_topic_subscriptions = flatten([
    for topic_name, subscriptions in { for v in local.service_bus_topics_and_subscriptions : v.name => v.subscriptions } : [
      for subscription_name, subscription in { for k, v in subscriptions : k => merge(local.service_bus_topic_subscriptions_defaults, v) } : {
        subscription_name                         = subscription_name
        topic_name                                = topic_name
        status                                    = subscription.status
        max_delivery_count                        = subscription.max_delivery_count
        auto_delete_on_idle                       = subscription.auto_delete_on_idle
        default_message_ttl                       = subscription.default_message_ttl
        lock_duration                             = subscription.lock_duration
        dead_lettering_on_message_expiration      = subscription.dead_lettering_on_message_expiration
        dead_lettering_on_filter_evaluation_error = subscription.dead_lettering_on_filter_evaluation_error
        enable_batched_operations                 = subscription.enable_batched_operations
        requires_session                          = subscription.requires_session
        forward_to                                = subscription.forward_to
        role_assignments                          = subscription.role_assignments
      }
    ]
  ])

  service_bus_null_principals = {
    users              = []
    groups             = []
    service_principals = []
  }

  service_bus_topic_subscriptions_roles = flatten([
    for subscription, roles in { for k, v in local.service_bus_topic_subscriptions : v.subscription_name => v.role_assignments } : [
      for role, principals in { for k, v in roles : k => merge(local.service_bus_null_principals, v) } : [
        for principal in principals : {
          role_definition_name    = role
          subscription_name       = subscription
          user_principal_names    = principals.users
          group_names             = principals.groups
          service_principal_names = principals.service_principals
        }
      ]
    ]
  ])

  service_bus_role_assignments = flatten([
    for role, principals in { for k, v in var.service_bus_role_assignments : k => merge(local.service_bus_null_principals, v) } : [
      for principal in principals : {
        role_definition_name    = role
        user_principal_names    = principals.users
        group_names             = principals.groups
        service_principal_names = principals.service_principals
      }
    ]
  ])

  user_principal_names    = distinct(flatten([for v in concat(local.service_bus_topic_subscriptions_roles, local.service_bus_role_assignments) : v.user_principal_names]))
  group_names             = distinct(flatten([for v in concat(local.service_bus_topic_subscriptions_roles, local.service_bus_role_assignments) : v.group_names]))
  service_principal_names = distinct(flatten([for v in concat(local.service_bus_topic_subscriptions_roles, local.service_bus_role_assignments) : v.service_principal_names]))

  user_object_ids              = { for u in data.azuread_user.users : lower(u.user_principal_name) => u.id }
  group_object_ids             = { for g in data.azuread_group.groups : lower(g.display_name) => g.object_id }
  service_principal_object_ids = { for s in data.azuread_service_principal.service_principals : lower(s.display_name) => s.id }

  service_bus_subscription_role_assignments = distinct(concat(
    flatten([
      for role in local.service_bus_topic_subscriptions_roles : [
        for user in role.user_principal_names : {
          role_definition_name = role.role_definition_name
          subscription_name    = role.subscription_name
          principal_id         = local.user_object_ids[lower(user)]
        }
      ]
    ]),
    flatten([
      for role in local.service_bus_topic_subscriptions_roles : [
        for group in role.group_names : {
          role_definition_name = role.role_definition_name
          subscription_name    = role.subscription_name
          principal_id         = local.group_object_ids[lower(group)]
        }
      ]
    ]),
    flatten([
      for role in local.service_bus_topic_subscriptions_roles : [
        for service_principal in role.service_principal_names : {
          role_definition_name = role.role_definition_name
          subscription_name    = role.subscription_name
          principal_id         = local.service_principal_object_ids[lower(service_principal)]
        }
      ]
    ])
  ))

  service_bus_roles = distinct(concat(
    flatten([
      for role in local.service_bus_role_assignments : [
        for user in role.user_principal_names : {
          role_definition_name = role.role_definition_name
          principal_id         = local.user_object_ids[lower(user)]
        }
      ]
    ]),
    flatten([
      for role in local.service_bus_role_assignments : [
        for group in role.group_names : {
          role_definition_name = role.role_definition_name
          principal_id         = local.group_object_ids[lower(group)]
        }
      ]
    ]),
    flatten([
      for role in local.service_bus_role_assignments : [
        for service_principal in role.service_principal_names : {
          role_definition_name = role.role_definition_name
          principal_id         = local.service_principal_object_ids[lower(service_principal)]
        }
      ]
    ])
  ))

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
}
