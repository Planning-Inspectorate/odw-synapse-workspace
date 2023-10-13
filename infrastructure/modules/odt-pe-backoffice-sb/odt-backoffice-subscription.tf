resource "azurerm_servicebus_subscription" "odt_backoffice_subscription" {
  name                                      = "service-user"
  topic_id                                  = data.azurerm_servicebus_topic.odt_backoffice_topic.id
  status                                    = "Active"
  max_delivery_count                        = 1
  auto_delete_on_idle                       = "P10675199DT2H48M5.4775807S"
  default_message_ttl                       = "P14D"
  lock_duration                             = "PT1M"
  dead_lettering_on_message_expiration      = false
  dead_lettering_on_filter_evaluation_error = true
  enable_batched_operations                 = false
  requires_session                          = false
  forward_to                                = ""
}

resource "azurerm_role_assignment" "odt_backoffice_subscription_role_assignments" {
   scope                = azurerm_servicebus_subscription.odt_backoffice_subscription.id
   role_definition_name = "Azure Service Bus Data Receiver"
   principal_id         = module.synapse_workspace_private.synapse_principal_id
}
