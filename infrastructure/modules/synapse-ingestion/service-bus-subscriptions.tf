# resource "azurerm_servicebus_subscription" "topic_subscriptions" {
#   for_each = var.failover_namespacee ? 0 : var.service_bus_topics_and_subscriptions == null ? 0 : var.service_bus_topics_and_subscriptions

#   name               = each.value
#   topic_id           = azurerm_servicebus_topic.topics[each.key].id
#   max_delivery_count = 1
# }
