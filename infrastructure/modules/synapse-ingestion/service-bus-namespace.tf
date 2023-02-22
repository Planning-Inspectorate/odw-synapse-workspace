resource "azurerm_servicebus_namespace" "synapse" {
  #checkov:skip=CKV_AZURE_199: Microsoft managed keys are acceptable
  #checkov:skip=CKV_AZURE_201: Microsoft managed keys are acceptable
  #checkov:skip=CKV_AZURE_203: Azure Service Bus Local Authentication should be enabled
  #checkov:skip=CKV_AZURE_204: Public access should be enabled
  name                = "pins-sb-${local.resource_suffix}-${random_string.unique_id.id}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.service_bus_failover_enabled ? "Premium" : "Standard"
  capacity            = var.service_bus_failover_enabled ? 1 : 0
  minimum_tls_version = "1.2"
  local_auth_enabled  = true

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}
