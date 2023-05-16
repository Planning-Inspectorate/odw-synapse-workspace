resource "azurerm_storage_account" "logic_app" {
  count = var.logic_app_enabled ? 1 : 0
  #checkov:skip=CKV_AZURE_35: Firewall not required for this storage account
  #checkov:skip=CKV_AZURE_59: Firewall not required for this storage account
  #checkov:skip=CKV_AZURE_190: Firewall not required for this storage account
  #checkov:skip=CKV_AZURE_206: Locally redundant storage is acceptable for this storage account use-case
  #checkov:skip=CKV2_AZURE_1: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_8: Firewall not required for this storage account
  #checkov:skip=CKV2_AZURE_18: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_33: Firewall not used on this storage account to implement private endpoint
  name                     = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"

  queue_properties {
    logging {
      read                  = true
      write                 = true
      delete                = true
      retention_policy_days = 7
      version               = "1.0"
    }

    minute_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = 7
      version               = "1.0"
    }

    hour_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = 7
      version               = "1.0"
    }
  }

  tags = local.tags
}
