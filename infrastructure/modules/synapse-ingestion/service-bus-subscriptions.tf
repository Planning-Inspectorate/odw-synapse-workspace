resource "azurerm_servicebus_subscription" "topic_subscriptions" {
  for_each = var.failover_namespace ? {} : {
    for topic in local.service_bus_topics_and_subscriptions : "${topic.topic_name}.${topic.subscription_name}" => topic
  }

  name               = each.value.subscription_name
  topic_id           = azurerm_servicebus_topic.topics[each.key].id
  max_delivery_count = 1
}
