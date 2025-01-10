resource "azurerm_servicebus_subscription" "topic_subscriptions" {
  for_each = {
    for subscription in local.service_bus_topic_subscriptions : subscription.subscription_name => subscription
  }

  name                                      = each.key
  topic_id                                  = azurerm_servicebus_topic.topics[each.value.topic_name].id
  status                                    = each.value.status
  max_delivery_count                        = each.value.max_delivery_count
  auto_delete_on_idle                       = each.value.auto_delete_on_idle
  default_message_ttl                       = each.value.default_message_ttl
  lock_duration                             = each.value.lock_duration
  dead_lettering_on_message_expiration      = each.value.dead_lettering_on_message_expiration
  dead_lettering_on_filter_evaluation_error = each.value.dead_lettering_on_filter_evaluation_error
  batched_operations_enabled                = each.value.enable_batched_operations
  requires_session                          = each.value.requires_session
  forward_to                                = each.value.forward_to
}
