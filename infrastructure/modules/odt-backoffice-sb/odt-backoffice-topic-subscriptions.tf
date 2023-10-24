resource "azurerm_servicebus_subscription" "odt_backoffice_subscriptions" {
  for_each = local.odt_backoffice_sb_subscriptions

  name                                      = each.key
  topic_id                                  = local.odt_backoffice_sb_topic_ids[lower(each.value.topic_name)]
  status                                    = each.value.status
  max_delivery_count                        = each.value.max_delivery_count
  auto_delete_on_idle                       = each.value.auto_delete_on_idle
  default_message_ttl                       = each.value.default_message_ttl
  lock_duration                             = each.value.lock_duration
  dead_lettering_on_message_expiration      = each.value.dead_lettering_on_message_expiration
  dead_lettering_on_filter_evaluation_error = each.value.dead_lettering_on_filter_evaluation_error
  enable_batched_operations                 = each.value.enable_batched_operations
  requires_session                          = each.value.requires_session
  forward_to                                = each.value.forward_to
}

resource "azurerm_role_assignment" "odt_backoffice_sb_subscription_role_assignments" {
  for_each = {
    for assignment in local.odt_backoffice_sb_roles : "${assignment.role_definition_name}.${assignment.principal_id}.${assignment.subscription_name}" => assignment
  }

  scope                = azurerm_servicebus_subscription.odt_backoffice_subscriptions[each.value.subscription_name].id
  role_definition_name = each.value.role_definition_name
  principal_id         = each.value.principal_id
}
