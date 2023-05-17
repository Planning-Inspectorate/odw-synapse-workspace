resource "azurerm_servicebus_topic" "topics" {
  for_each = local.service_bus_topics_and_subscriptions

  name                                    = each.value.name
  namespace_id                            = azurerm_servicebus_namespace.synapse.id
  status                                  = each.value.status
  auto_delete_on_idle                     = each.value.auto_delete_on_idle
  default_message_ttl                     = each.value.default_message_ttl
  duplicate_detection_history_time_window = each.value.duplicate_detection_history_time_window
  enable_batched_operations               = each.value.enable_batched_operations
  enable_express                          = each.value.enable_express
  enable_partitioning                     = each.value.enable_partitioning
  max_size_in_megabytes                   = each.value.max_size_in_megabytes
  requires_duplicate_detection            = each.value.requires_duplicate_detection
  support_ordering                        = each.value.support_ordering
}
