resource "azurerm_servicebus_subscription" "topic_subscriptions" {
  for_each = var.failover_namespace ? {} : {
    for subscription in local.service_bus_topics_and_subscriptions : "${subscription.topic_name}.${subscription.subscription_name}" => subscription
  }
  
  name               = each.value.subscription_name
  topic_id           = azurerm_servicebus_topic.topics[each.value.topic_name].id
  max_delivery_count = 1
}
