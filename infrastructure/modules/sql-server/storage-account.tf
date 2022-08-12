resource "azurerm_storage_account" "sql_server_auditing" {
  #checkov:skip=CKV_AZURE_35:   TODO: Set default network access to deny
  #checkov:skip=CKV2_AZURE_1:   TODO: Implement customer-managed keys for encryption
  #checkov:skip=CKV2_AZURE_8:   TODO: Set activity log container to private
  #checkov:skip=CKV2_AZURE_18:  TODO: Implement customer-managed keys for encryption
  name                      = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  location                  = var.location
  resource_group_name       = var.resource_group_name
  account_tier              = "Standard"
  account_replication_type  = "LRS"
  account_kind              = "StorageV2"
  enable_https_traffic_only = true
  min_tls_version           = "TLS1_2"

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
