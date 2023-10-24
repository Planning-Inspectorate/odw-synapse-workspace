locals {
  module_name     = "odt-backoffice-sb"
  resource_suffix = "${var.service_name}-${var.environment}-${module.azure_region.location_short}"

  tags = merge(
    var.tags,
    {
      ModuleName = local.module_name
    }
  )
  odt_backoffice_sb_topic_subscriptions_defaults = {
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

  odt_backoffice_sb_subscriptions = { for v in var.odt_backoffice_sb_topic_subscriptions : v.subscription_name => merge(local.odt_backoffice_sb_topic_subscriptions_defaults, v) }

  odt_backoffice_sb_topic_names = distinct(flatten([for v in var.odt_backoffice_sb_topic_subscriptions : v.topic_name]))
  odt_backoffice_sb_topic_ids   = { for t in data.azurerm_servicebus_topic.topic_id : t.name => t.id }

  service_bus_null_principals = {
    users              = []
    groups             = []
    service_principals = []
  }

  odt_backoffice_sb_topic_subscriptions_roles = flatten([
    for subscription, roles in { for k, v in local.odt_backoffice_sb_subscriptions : v.subscription_name => v.role_assignments } : [
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

  user_principal_names    = distinct(flatten([for v in local.odt_backoffice_sb_topic_subscriptions_roles : v.user_principal_names]))
  group_names             = distinct(flatten([for v in local.odt_backoffice_sb_topic_subscriptions_roles : v.group_names]))
  service_principal_names = distinct(flatten([for v in local.odt_backoffice_sb_topic_subscriptions_roles : v.service_principal_names]))

  user_principal_ids    = { for v in data.azuread_user.users : v.user_principal_name => v.id }
  group_ids             = { for v in data.azuread_group.groups : v.display_name => v.id }
  service_principal_ids = { for v in data.azuread_service_principal.service_principals : v.display_name => v.id }

  odt_backoffice_sb_roles = distinct(concat(
    flatten([
      for v in local.odt_backoffice_sb_topic_subscriptions_roles : [
        for user_principal_name in v.user_principal_names : {
          subscription_name    = v.subscription_name
          role_definition_name = v.role_definition_name
          principal_id         = local.user_principal_ids[user_principal_name]
        }
      ]
    ]),
    flatten([
      for v in local.odt_backoffice_sb_topic_subscriptions_roles : [
        for group_name in v.group_names : {
          subscription_name    = v.subscription_name
          role_definition_name = v.role_definition_name
          principal_id         = local.group_ids[group_name]
        }
      ]
    ]),
    flatten([
      for v in local.odt_backoffice_sb_topic_subscriptions_roles : [
        for service_principal_name in v.service_principal_names : {
          subscription_name    = v.subscription_name
          role_definition_name = v.role_definition_name
          principal_id         = local.service_principal_ids[service_principal_name]
        }
      ]
    ])
  ))
}
