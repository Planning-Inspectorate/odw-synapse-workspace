resource "azurerm_servicebus_subscription" "topic_subscriptions" {
  count = var.failover_namespace ? 0 : var.service_bus_topics_and_subscriptions == null ? 0 : length(var.service_bus_topics_and_subscriptions)

  name               = element(values(var.service_bus_topics_and_subscriptions), count.index)
  topic_id           = azurerm_servicebus_topic.topics[count.index].id
  max_delivery_count = 1
}
