resource "azurerm_servicebus_topic" "topics" {
  count = var.failover_namespace ? 0 : var.service_bus_topics_and_subscriptions == null ? 0 : length(var.service_bus_topics_and_subscriptions)

  name         = keys(var.service_bus_topics_and_subscriptions)[count.index]
  namespace_id = azurerm_servicebus_namespace.synapse.id

  auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
  default_message_ttl                     = "P14D"
  duplicate_detection_history_time_window = "P7D"
  enable_batched_operations               = true
  enable_partitioning                     = false
  max_size_in_megabytes                   = 1024 # Attribute name incorrect: 1024 = 16GB
  requires_duplicate_detection            = true
}
