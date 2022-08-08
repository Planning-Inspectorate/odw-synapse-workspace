resource "azurerm_servicebus_topic" "employee" {
  name         = "sbt-employee-${local.resource_suffix}"
  namespace_id = azurerm_servicebus_namespace.synapse.id

  auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
  default_message_ttl                     = "P14D"
  duplicate_detection_history_time_window = "P7D"
  enable_batched_operations               = true
  enable_partitioning                     = true
  max_size_in_megabytes                   = 2048
  requires_duplicate_detection            = true
}
