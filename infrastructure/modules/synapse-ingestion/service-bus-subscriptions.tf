resource "azurerm_servicebus_subscription" "topic_subscriptions" {
  for_each = var.failover_namespace ? {} : {
    for subscriptions in var.service_bus_topics_and_subscriptions : "${subscriptions.topic_name}.${subscriptions.subscription_name}" => subscriptions
  }

  name               = each.value.subscription_name
  topic_id           = azurerm_servicebus_topic.topics[each.key].id
  max_delivery_count = 1
}
